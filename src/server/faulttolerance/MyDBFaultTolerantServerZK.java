package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;

    private static final String FWD_PREFIX = "FWD:";

    private final NodeConfig<String> nodeConfig;
    private final String myID;
    private final MessageNIOTransport<String, byte[]> transport;
    private final Cluster cluster;

    private final Map<String, Session> sessions = new HashMap<>();
    private final List<String> sortedNodeIds;
    private volatile String currentLeader;

    /** NEW: Track dead servers */
    private final Set<String> deadServers = new HashSet<>();

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress isaDB) throws IOException {

        super(
                new InetSocketAddress(
                        nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET
                ),
                isaDB,
                myID
        );

        this.nodeConfig = nodeConfig;
        this.myID = myID;

        this.sortedNodeIds = new ArrayList<>(nodeConfig.getNodeIDs());
        Collections.sort(this.sortedNodeIds);
        this.currentLeader = this.sortedNodeIds.get(0);

        AbstractBytePacketDemultiplexer demux = new AbstractBytePacketDemultiplexer() {
            @Override
            public boolean handleMessage(byte[] bytes, NIOHeader header) {
                handleServerMessage(bytes);
                return true;
            }
        };

        this.transport = new MessageNIOTransport<>(
                nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID),
                demux
        );

        this.cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();

        for (String id : nodeConfig.getNodeIDs()) {
            Session tmp = cluster.connect();
            tmp.execute("CREATE KEYSPACE IF NOT EXISTS " + id +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            tmp.close();

            Session s = cluster.connect(id);

            s.execute("CREATE TABLE IF NOT EXISTS grade (id int PRIMARY KEY, events list<int>);");
            s.execute("CREATE TABLE IF NOT EXISTS grade_backup (id int PRIMARY KEY, events list<int>);");

            restoreFromBackupIfNeeded(id, s);
            sessions.put(id, s);
        }
    }

    private void restoreFromBackupIfNeeded(String keyspaceId, Session s) {
        try {
            if (s.execute("SELECT id FROM grade LIMIT 1;").one() != null) return;

            List<Row> backupRows = s.execute("SELECT id, events FROM grade_backup;").all();
            for (Row r : backupRows) {
                int id = r.getInt("id");
                List<Integer> events = r.getList("events", Integer.class);
                s.execute("INSERT INTO grade (id, events) VALUES (" + id + "," + events + ");");
            }
        } catch (Exception ignored) {}
    }

    private boolean isLeader() {
        return myID.equals(currentLeader);
    }

    private synchronized void advanceLeader(String failed) {
        int idx = sortedNodeIds.indexOf(failed);
        currentLeader = sortedNodeIds.get((idx + 1) % sortedNodeIds.size());
    }

    private static class EventUpdate {
        final int keyId, eventVal;
        EventUpdate(int k, int e) { keyId = k; eventVal = e; }
    }

    private EventUpdate parseEventsUpdate(String cmd) {
        try {
            String l = cmd.toLowerCase();
            if (!l.contains("events=events+[")) return null;
            int e = Integer.parseInt(cmd.substring(cmd.indexOf("[")+1, cmd.indexOf("]")));
            int k = Integer.parseInt(cmd.substring(l.indexOf("where id=")+9).replace(";",""));
            return new EventUpdate(k,e);
        } catch (Exception e) {
            return null;
        }
    }

    private void applyDeterministicEventsUpdate(Session s, EventUpdate ev) {
        List<Integer> events = Optional.ofNullable(
                s.execute("SELECT events FROM grade WHERE id="+ev.keyId+";").one())
                .map(r -> r.getList("events", Integer.class))
                .orElse(new ArrayList<>());
        events = new ArrayList<>(events);
        events.add(ev.eventVal);
        Collections.sort(events);
        s.execute("UPDATE grade SET events="+events+" WHERE id="+ev.keyId+";");
        s.execute("UPDATE grade_backup SET events="+events+" WHERE id="+ev.keyId+";");
    }

    private void executeOnReplica(Session s, String cmd) {
        EventUpdate ev = parseEventsUpdate(cmd);
        if (ev != null) applyDeterministicEventsUpdate(s, ev);
        else s.execute(cmd);
    }

    /** UPDATED: skip dead servers */
    private void executeOnAllReplicas(String cmd) {
        for (Map.Entry<String, Session> e : sessions.entrySet()) {
            if (deadServers.contains(e.getKey())) continue;
            executeOnReplica(e.getValue(), cmd);
        }
    }

    /** UPDATED: skip dead servers */
    private synchronized void applyInsertOnAllReplicas(String cmd) {
        for (Map.Entry<String, Session> e : sessions.entrySet()) {
            if (deadServers.contains(e.getKey())) continue;
            e.getValue().execute(cmd);
        }
    }

    private synchronized void applyAsLeader(String cmd) {
        executeOnAllReplicas(cmd);
    }

    @Override
    protected synchronized void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String cmd = new String(bytes, StandardCharsets.UTF_8).trim();
        if (cmd.toLowerCase().startsWith("insert into grade")) {
            applyInsertOnAllReplicas(cmd);
        } else if (isLeader()) {
            applyAsLeader(cmd);
        } else {
            forwardToLeader(cmd);
        }
    }

    /** UPDATED: mark leader dead on failure */
    private void forwardToLeader(String cmd) {
        while (true) {
            String target = currentLeader;
            if (myID.equals(target)) {
                applyAsLeader(cmd);
                return;
            }
            try {
                transport.send(
                        new InetSocketAddress(nodeConfig.getNodeAddress(target),
                                nodeConfig.getNodePort(target)),
                        (FWD_PREFIX + cmd).getBytes(StandardCharsets.UTF_8)
                );
                return;
            } catch (IOException e) {
                deadServers.add(target);
                advanceLeader(target);
            }
        }
    }

    private void handleServerMessage(byte[] bytes) {
        String msg = new String(bytes, StandardCharsets.UTF_8).trim();
        if (msg.startsWith(FWD_PREFIX))
            applyAsLeader(msg.substring(FWD_PREFIX.length()));
    }

    @Override
    public void close() {
        super.close();
        sessions.values().forEach(Session::close);
        cluster.close();
    }

    public static void main(String[] args) throws IOException {
        NodeConfig<String> nc = NodeConfigUtils.getNodeConfigFromFile(
                args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
        new MyDBFaultTolerantServerZK(nc, args[1],
                new InetSocketAddress("localhost", 9042));
    }
}
