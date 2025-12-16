package server.faulttolerance;

import com.datastax.driver.core.*;
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

/**
 * Fault-tolerant database server using a custom, leader-based,
 * server-to-server protocol layered on top of Cassandra.
 *
 * High-level behavior:
 *  - One logical leader (smallest ID in sorted server list) serializes updates.
 *  - Clients may send CQL to any server.
 *      * Non-leaders forward commands to the leader via MessageNIOTransport.
 *      * The leader executes each command on ALL Cassandra keyspaces
 *        (one per logical server) so state converges everywhere.
 *  - For UPDATEs of the form:
 *        UPDATE grade SET events = events + [X] WHERE id = Y;
 *    the leader performs a deterministic read-modify-write:
 *      * read list, append X, sort, write back full list.
 *    This removes Cassandra's non-deterministic list-append behavior.
 *  - INSERT INTO grade ... commands:
 *      * may be accepted by any server,
 *      * are applied on all keyspaces,
 *      * and mirrored into grade_backup for checkpointing.
 *  - On startup, grade can be reconstructed from grade_backup
 *    if grade is empty but backup is not.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    /** Used by tests to sleep between operations. */
    public static final int SLEEP = 1000;

    /** Tests may look at this flag at the end (drop tables or not). */
    public static final boolean DROP_TABLES_AFTER_TESTS = true;

    /** Prefix for server-to-server forwarded messages. */
    private static final String FWD_PREFIX = "FWD:";

    /** Cluster configuration (node IDs, addresses, ports). */
    private final NodeConfig<String> nodeConfig;

    /** This server's logical ID (e.g., "server0"). */
    private final String myID;

    /** Transport used ONLY for server-to-server messages (FWD:...). */
    private final MessageNIOTransport<String, byte[]> transport;

    /** Shared Cassandra cluster handle. */
    private final Cluster cluster;

    /**
     * Map from logical server ID (keyspace name) → Cassandra Session bound to
     * that keyspace. The leader uses these Sessions to apply updates on all
     * logical replicas.
     */
    private final Map<String, Session> sessions = new HashMap<>();

    /**
     * Sorted list of node IDs (["server0","server1","server2",...]) used for
     * deterministic initial leader and simple round-robin leader changes.
     */
    private final List<String> sortedNodeIds;

    /**
     * Who this process currently believes is the leader.
     * Starts as the smallest ID in sortedNodeIds and may advance on
     * forwarding failures.
     */
    private volatile String currentLeader;

    /**
     * Set of logical server IDs whose PROCESS has been observed as dead
     * (forwarding to them failed). This is used only for leader selection.
     *
     * NOTE: We still write to ALL Cassandra keyspaces, even for dead servers.
     * Cassandra itself is a separate, always-on backing store.
     */
    private final Set<String> deadServers = new HashSet<>();

    /* ---------------------------------------------------------------------- */
    /* Constructor                                                            */
    /* ---------------------------------------------------------------------- */

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress isaDB) throws IOException {

        // Start the underlying single-server (client-facing) TCP server.
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

        // Deterministic ordering of node IDs and initial leader.
        this.sortedNodeIds = new ArrayList<>(nodeConfig.getNodeIDs());
        Collections.sort(this.sortedNodeIds);
        this.currentLeader = this.sortedNodeIds.get(0);

        // Demultiplexer for server-to-server messages.
        AbstractBytePacketDemultiplexer demux = new AbstractBytePacketDemultiplexer() {
            @Override
            public boolean handleMessage(byte[] bytes, NIOHeader header) {
                handleServerMessage(bytes);
                return true;
            }
        };

        // Server-to-server transport uses the "plain" port (no offset).
        this.transport = new MessageNIOTransport<>(
                nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID),
                demux
        );

        // Single Cassandra cluster for all keyspaces.
        this.cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();

        // For each logical server ID:
        //  - Ensure keyspace exists.
        //  - Create a Session bound to that keyspace.
        //  - Ensure grade + grade_backup tables exist.
        //  - Try to restore grade from backup if empty.
        for (String id : nodeConfig.getNodeIDs()) {
            Session tmp = cluster.connect();
            tmp.execute("CREATE KEYSPACE IF NOT EXISTS " + id +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            tmp.close();

            Session s = cluster.connect(id);

            s.execute("CREATE TABLE IF NOT EXISTS grade (" +
                    "id int PRIMARY KEY, " +
                    "events list<int>)");

            s.execute("CREATE TABLE IF NOT EXISTS grade_backup (" +
                    "id int PRIMARY KEY, " +
                    "events list<int>)");

            restoreFromBackupIfNeeded(id, s);
            sessions.put(id, s);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Startup restore from backup                                            */
    /* ---------------------------------------------------------------------- */

    /**
     * If grade is empty but grade_backup has rows, reconstruct grade
     * from grade_backup for the given keyspace.
     */
    private void restoreFromBackupIfNeeded(String keyspaceId, Session s) {
        try {
            // If grade already has data, nothing to restore.
            Row row = s.execute("SELECT id FROM grade LIMIT 1").one();
            if (row != null) {
                return;
            }

            List<Row> backupRows =
                    s.execute("SELECT id, events FROM grade_backup").all();
            if (backupRows.isEmpty()) {
                return;
            }

            for (Row r : backupRows) {
                int id = r.getInt("id");
                List<Integer> events = r.getList("events", Integer.class);
                // Using events.toString() is valid CQL literal for list<int>
                s.execute("INSERT INTO grade (id, events) VALUES (" +
                        id + "," + events + ")");
            }

            System.out.println("[" + myID + "] restored keyspace " + keyspaceId +
                    " from grade_backup with " + backupRows.size() + " rows.");
        } catch (Exception e) {
            System.err.println("[" + myID + "] restoreFromBackupIfNeeded failed for " +
                    keyspaceId + ": " + e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Leader bookkeeping                                                     */
    /* ---------------------------------------------------------------------- */

    private boolean isLeader() {
        return myID.equals(currentLeader);
    }

    /**
     * Advance currentLeader to the next ID in sortedNodeIds. Also records the
     * failed server in deadServers (used for future forwarding decisions).
     */
    private synchronized void advanceLeader(String failed) {
        deadServers.add(failed);
        int idx = sortedNodeIds.indexOf(failed);
        if (idx < 0) {
            return;
        }
        int nextIdx = (idx + 1) % sortedNodeIds.size();
        currentLeader = sortedNodeIds.get(nextIdx);
        System.err.println("[" + myID + "] leader advanced from " + failed +
                " to " + currentLeader);
    }

    /* ---------------------------------------------------------------------- */
    /* Deterministic events=events+[X] parsing and application                */
    /* ---------------------------------------------------------------------- */

    /**
     * Holds (id, eventVal) for an UPDATE of the form:
     *   UPDATE grade SET events = events + [X] WHERE id = Y;
     */
    private static class EventUpdate {
        final int keyId;
        final int eventVal;

        EventUpdate(int keyId, int eventVal) {
            this.keyId = keyId;
            this.eventVal = eventVal;
        }
    }

    /**
     * Robustly parse updates like:
     *   UPDATE grade SET events = events + [X] WHERE id = Y;
     *   update grade set events=events+[X] where id=Y;
     *   (any spacing / case variant)
     *
     * If the pattern does not match, returns null.
     */
    private EventUpdate parseEventsUpdate(String cmd) {
        String lower = cmd.toLowerCase();
        if (!lower.contains("update") || !lower.contains("set") || !lower.contains("where")) {
            return null;
        }

        try {
            // Split into SET part and WHERE part.
            int setIdx = lower.indexOf("set");
            int whereIdx = lower.indexOf("where", setIdx);
            if (setIdx < 0 || whereIdx < 0) return null;

            String setPart = cmd.substring(setIdx + 3, whereIdx);   // between SET and WHERE
            String wherePart = cmd.substring(whereIdx + 5);         // after WHERE

            // Normalize SET part by stripping all whitespace.
            String setNoSpace = setPart.replaceAll("\\s+", "").toLowerCase();
            // Expect: events=events+[X]
            String marker = "events=events+[";
            int idx = setNoSpace.indexOf(marker);
            if (idx < 0) return null;
            int startEv = idx + marker.length();
            int endEv = setNoSpace.indexOf("]", startEv);
            if (endEv < 0) return null;

            String eventStr = setNoSpace.substring(startEv, endEv).trim();
            int eventVal = Integer.parseInt(eventStr);

            // Parse WHERE id = Y (allow spaces around '=' and negative IDs).
            String whereLower = wherePart.toLowerCase();
            int idIdx = whereLower.indexOf("id");
            if (idIdx < 0) return null;
            int eqIdx = whereLower.indexOf("=", idIdx);
            if (eqIdx < 0) return null;

            int pos = eqIdx + 1;
            // Skip any whitespace.
            while (pos < wherePart.length() && Character.isWhitespace(wherePart.charAt(pos))) {
                pos++;
            }
            int startKey = pos;
            // Include optional '-' and digits.
            while (pos < wherePart.length()) {
                char c = wherePart.charAt(pos);
                if ((c == '-' && pos == startKey) || Character.isDigit(c)) {
                    pos++;
                } else {
                    break;
                }
            }
            String keyStr = wherePart.substring(startKey, pos).trim();
            int keyId = Integer.parseInt(keyStr);

            return new EventUpdate(keyId, eventVal);
        } catch (Exception e) {
            // If anything goes wrong, treat as non-matching.
            return null;
        }
    }

    /**
     * Deterministically apply an "events = events + [X]" update on a single keyspace:
     *   - SELECT events FROM grade WHERE id = key
     *   - Convert to List<Integer> (or empty list if null)
     *   - Add X, sort the list
     *   - Write full list back into grade and grade_backup
     */
    private void applyDeterministicEventsUpdate(Session s, EventUpdate ev) {
        try {
            Row r = s.execute("SELECT events FROM grade WHERE id = " + ev.keyId).one();
            List<Integer> events;

            if (r == null || r.isNull("events")) {
                events = new ArrayList<>();
            } else {
                events = new ArrayList<>(r.getList("events", Integer.class));
            }

            events.add(ev.eventVal);
            Collections.sort(events);

            // Using list.toString() yields valid CQL literal for list<int>: [1, 2, 3]
            s.execute("INSERT INTO grade (id, events) VALUES (" +
                    ev.keyId + "," + events + ")");
            s.execute("INSERT INTO grade_backup (id, events) VALUES (" +
                    ev.keyId + "," + events + ")");
        } catch (Exception e) {
            System.err.println("[" + myID + "] deterministic events update failed: " + e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Replication helpers                                                    */
    /* ---------------------------------------------------------------------- */

    /**
     * Execute a single CQL command on *one* replica (one keyspace).
     * If it matches the events=events+[X] pattern, we use the deterministic path.
     * Otherwise we just execute the raw CQL.
     */
    private void executeOnReplica(Session s, String cmd) {
        String trimmed = cmd.trim();
        if (trimmed.isEmpty()) return;

        EventUpdate ev = parseEventsUpdate(trimmed);
        if (ev != null) {
            applyDeterministicEventsUpdate(s, ev);
            return;
        }

        try {
            s.execute(trimmed);
        } catch (Exception e) {
            System.err.println("[" + myID + "] CQL execution failed on replica: " + e);
        }
    }

    /**
     * Execute the command on ALL logical replicas (all keyspaces).
     * We DO NOT skip keyspaces whose server processes have crashed:
     * Cassandra is shared and always up, so we keep all replicas' data in sync.
     */
    private void executeOnAllReplicas(String cmd) {
        for (Session s : sessions.values()) {
            executeOnReplica(s, cmd);
        }
    }

    /**
     * Apply INSERT INTO grade ... on all replicas and mirror into grade_backup.
     */
    private synchronized void applyInsertOnAllReplicas(String cmd) {
        try {
            // We need the key id to populate backup table.
            int keyId = parseInsertKeyId(cmd);

            for (Session s : sessions.values()) {
                // Execute the original INSERT first.
                s.execute(cmd);

                if (keyId < 0) {
                    continue;
                }

                // Fetch back events to mirror into backup.
                Row r = s.execute(
                        "SELECT events FROM grade WHERE id = " + keyId).one();

                List<Integer> events;
                if (r == null || r.isNull("events")) {
                    events = new ArrayList<>();
                } else {
                    events = new ArrayList<>(r.getList("events", Integer.class));
                }

                s.execute("INSERT INTO grade_backup (id, events) VALUES (" +
                        keyId + "," + events + ")");
            }
        } catch (Exception e) {
            System.err.println("[" + myID + "] insert-on-all-replicas failed: " + e);
        }
    }

    /**
     * Parse the primary key ID out of an INSERT INTO grade ... VALUES (...) command.
     * Returns -1 if parsing fails.
     */
    private int parseInsertKeyId(String cmd) {
        try {
            String lower = cmd.toLowerCase();
            if (!lower.startsWith("insert into grade")) return -1;

            int idxValues = lower.indexOf("values");
            if (idxValues < 0) return -1;

            int open = cmd.indexOf("(", idxValues);
            int close = cmd.indexOf(")", open);
            if (open < 0 || close < 0) return -1;

            String inside = cmd.substring(open + 1, close);
            // First value is id
            String[] parts = inside.split(",", 2);
            if (parts.length == 0) return -1;

            return Integer.parseInt(parts[0].trim());
        } catch (Exception e) {
            return -1;
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Leader application + client handler                                    */
    /* ---------------------------------------------------------------------- */

    /**
     * Apply a command as leader: serialize and replicate to all keyspaces.
     */
    private synchronized void applyAsLeader(String cmd) {
        executeOnAllReplicas(cmd);
    }

    /**
     * Handle client requests:
     *  - INSERT INTO grade ... → apply on all replicas from any node.
     *  - Other commands:
     *      * If leader: applyAsLeader(cmd)
     *      * Else: forwardToLeader(cmd)
     */
    @Override
    protected synchronized void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String cmd = new String(bytes, StandardCharsets.UTF_8).trim();
        if (cmd.isEmpty()) return;

        String lower = cmd.toLowerCase();

        if (lower.startsWith("insert into grade")) {
            applyInsertOnAllReplicas(cmd);
            return;
        }

        if (isLeader()) {
            applyAsLeader(cmd);
        } else {
            forwardToLeader(cmd);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Forwarding + server-to-server handler                                  */
    /* ---------------------------------------------------------------------- */

    /**
     * Forward a client command to the current leader. If the leader is down,
     * we mark it dead, advance to the next candidate, and retry until either:
     *  - we become the leader and execute locally, or
     *  - a live leader successfully receives the forwarded command.
     */
    private void forwardToLeader(String cmd) {
        while (true) {
            String target = currentLeader;

            if (myID.equals(target)) {
                // After some failures we might have promoted ourselves.
                applyAsLeader(cmd);
                return;
            }

            InetSocketAddress addr = new InetSocketAddress(
                    nodeConfig.getNodeAddress(target),
                    nodeConfig.getNodePort(target)
            );

            String wrapped = FWD_PREFIX + cmd;
            try {
                transport.send(addr, wrapped.getBytes(StandardCharsets.UTF_8));
                return;
            } catch (IOException e) {
                System.err.println("[" + myID + "] forward to leader " + target +
                        " failed: " + e.getMessage());
                advanceLeader(target);
            }
        }
    }

    /**
     * Handle server-to-server messages (currently only FWD:<CQL>).
     */
    private void handleServerMessage(byte[] bytes) {
        String msg = new String(bytes, StandardCharsets.UTF_8).trim();
        if (msg.isEmpty()) return;

        if (msg.startsWith(FWD_PREFIX)) {
            String cmd = msg.substring(FWD_PREFIX.length());
            applyAsLeader(cmd);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Shutdown + main                                                        */
    /* ---------------------------------------------------------------------- */

    @Override
    public void close() {
        super.close();
        try {
            for (Session s : sessions.values()) {
                try {
                    s.close();
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}

        try {
            if (cluster != null) cluster.close();
        } catch (Exception ignored) {}
    }

    /**
     * Main entry used by ServerFailureRecoveryManager.
     * args[0] = node config file path
     * args[1] = this node's logical ID (e.g., "server0")
     */
    public static void main(String[] args) throws IOException {
        NodeConfig<String> nc = NodeConfigUtils.getNodeConfigFromFile(
                args[0],
                ReplicatedServer.SERVER_PREFIX,
                ReplicatedServer.SERVER_PORT_OFFSET
        );

        String myID = args[1];
        InetSocketAddress dbAddress =
                new InetSocketAddress("localhost", 9042);

        new MyDBFaultTolerantServerZK(nc, myID, dbAddress);
    }
}
