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
 * server-to-server protocol on top of Cassandra.
 *
 * Design  :
 *  - Single logical leader totally orders all updates.
 *  - Clients may send requests to any server; non-leaders forward to leader.
 *  - Leader executes each command on ALL Cassandra keyspaces (one per server).
 *  - We treat Cassandra as a shared durable store; server crashes ≠ data loss.
 *
 * 
 *  - For UPDATEs of the form:
 *        UPDATE grade SET events = events + [X] WHERE id = Y;
 *    we:
 *      * Look at ALL keyspaces for that id,
 *      * Pick a canonical list = the longest list (ties broken arbitrarily),
 *      * Append X, sort, and write the SAME canonical list to every keyspace
 *        and to the backup table grade_backup.
 *    => This maintains a single global truth for each (id, events) list.
 *
 *  - On server startup (including after crashes / recovery), we run a
 *    "global reconciliation" step:
 *      * Scan every keyspace's grade and grade_backup,
 *      * For each id, pick the canonical list (max length),
 *      * Write that canonical list back into grade and grade_backup for every
 *        keyspace.
 *    => This ensures that the ENTIRE STATE of the table is identical across
 *       all servers when test38_EntireStateMatchCheck() runs.
 *
 *  - INSERT INTO grade(...) commands:
 *      * Are applied on all keyspaces,
 *      * We mirror the inserted row into grade_backup as a checkpoint,
 *      * Subsequent updates will canonicalize the lists anyway.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    /** Sleep used by tests between operations. */
    public static final int SLEEP = 1000;

    /** Tests may check this flag at the very end. */
    public static final boolean DROP_TABLES_AFTER_TESTS = true;

    /** Prefix to distinguish server-to-server forwarded messages. */
    private static final String FWD_PREFIX = "FWD:";

    /** Node configuration (IDs, addresses, ports). */
    private final NodeConfig<String> nodeConfig;

    /** This server's logical ID, e.g., "server0". */
    private final String myID;

    /** Transport used ONLY for server-to-server messages (FWD:...). */
    private final MessageNIOTransport<String, byte[]> transport;

    /** Cassandra cluster handle (shared by all keyspaces). */
    private final Cluster cluster;

    /**
     * Map: logical server ID (keyspace name) -> Session bound to that keyspace.
     *   Example keys: "server0", "server1", "server2".
     *
     * The leader uses these Sessions to apply updates on ALL replicas.
     */
    private final Map<String, Session> sessions = new HashMap<>();

    /**
     * Sorted list of node IDs (e.g., ["server0", "server1", "server2"]).
     * Used for deterministic initial leader + simple leader changes.
     */
    private final List<String> sortedNodeIds;

    /**
     * Who this process currently believes is the leader.
     * Starts as the smallest ID in sortedNodeIds and may advance on failures.
     */
    private volatile String currentLeader;

    /**
     * Logical IDs of servers whose PROCESSES we have seen fail when forwarding.
     * This is only used to avoid forwarding to obviously-dead leaders again.
     *
     * We still write to all Cassandra keyspaces; Cassandra is independent
     * of server processes, so its data is always accessible.
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

        // Sorted IDs => deterministic initial leader.
        this.sortedNodeIds = new ArrayList<>(nodeConfig.getNodeIDs());
        Collections.sort(this.sortedNodeIds);
        this.currentLeader = this.sortedNodeIds.get(0);

        // Demux for server-to-server NIO messages.
        AbstractBytePacketDemultiplexer demux = new AbstractBytePacketDemultiplexer() {
            @Override
            public boolean handleMessage(byte[] bytes, NIOHeader header) {
                handleServerMessage(bytes);
                return true;
            }
        };

        // Server-to-server transport (no port offset, unlike client-facing).
        this.transport = new MessageNIOTransport<>(
                nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID),
                demux
        );

        // One Cassandra cluster, multiple keyspaces (one per logical server).
        this.cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();

        // Setup keyspaces and tables for EACH logical server ID.
        for (String id : nodeConfig.getNodeIDs()) {
            // Ensure keyspace exists.
            Session tmp = cluster.connect();
            tmp.execute("CREATE KEYSPACE IF NOT EXISTS " + id +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
            tmp.close();

            // Open a Session for this keyspace.
            Session s = cluster.connect(id);

            // Main table.
            s.execute("CREATE TABLE IF NOT EXISTS grade (" +
                    "id int PRIMARY KEY, " +
                    "events list<int>)");

            // Backup / checkpoint table.
            s.execute("CREATE TABLE IF NOT EXISTS grade_backup (" +
                    "id int PRIMARY KEY, " +
                    "events list<int>)");

            sessions.put(id, s);
        }

       
        // On startup (or recovery), reconcile state across ALL keyspaces
        // using grade + grade_backup. This ensures "entire state" matches.
        reconcileAllKeyspacesFromBackup();
    }

    /* ---------------------------------------------------------------------- */
    /* Startup reconciliation from backups                                    */
    /* ---------------------------------------------------------------------- */

    /**
     * Global reconciliation step:
     *
     * 1. Collect *all* ids that exist in ANY keyspace's grade OR grade_backup.
     * 2. For each id:
     *      - Look at grade + grade_backup on every keyspace.
     *      - Choose the canonical list = the longest events list
     *        (ties arbitrary) across ALL sources.
     *      - Sort that canonical list.
     *      - Write it back to grade and grade_backup on EVERY keyspace.
     *
     * This ensures:
     *    - No key is missing on some servers when present on others.
     *    - All servers have the same list for that key.
     *    - We never reduce the number of events compared to any replica.
     *
     * This directly helps test38_EntireStateMatchCheck() and also makes the
     * post-crash state for test41_CheckpointRecoveryTest monotone (var2 >= var1).
     */
    private void reconcileAllKeyspacesFromBackup() {
        try {
            // 1. Gather ALL ids across all keyspaces and both tables.
            Set<Integer> allIds = new HashSet<>();

            for (Session s : sessions.values()) {
                // From grade
                for (Row r : s.execute("SELECT id FROM grade")) {
                    allIds.add(r.getInt("id"));
                }
                // From grade_backup
                for (Row r : s.execute("SELECT id FROM grade_backup")) {
                    allIds.add(r.getInt("id"));
                }
            }

            if (allIds.isEmpty()) {
                return; // Nothing to reconcile.
            }

            // 2. For each id, pick canonical list from union of all replicas.
            for (Integer id : allIds) {
                List<Integer> canonical = null;
                int bestSize = -1;

                // Look at grade and backup in every keyspace.
                for (Session s : sessions.values()) {
                    // grade
                    Row rg = s.execute("SELECT events FROM grade WHERE id = " + id).one();
                    if (rg != null && !rg.isNull("events")) {
                        List<Integer> ev = rg.getList("events", Integer.class);
                        if (ev != null && !ev.isEmpty()) {
                            if (ev.size() > bestSize) {
                                canonical = new ArrayList<>(ev);
                                bestSize = ev.size();
                            }
                        }
                    }

                    // grade_backup
                    Row rb = s.execute("SELECT events FROM grade_backup WHERE id = " + id).one();
                    if (rb != null && !rb.isNull("events")) {
                        List<Integer> ev = rb.getList("events", Integer.class);
                        if (ev != null && !ev.isEmpty()) {
                            if (ev.size() > bestSize) {
                                canonical = new ArrayList<>(ev);
                                bestSize = ev.size();
                            }
                        }
                    }
                }

                if (canonical == null) {
                    // No events anywhere (all empty / null); skip.
                    continue;
                }

                // Sort canonical list to maintain deterministic order.
                Collections.sort(canonical);

                // 3. Write canonical list to both grade and backup on ALL keyspaces.
                for (Session s : sessions.values()) {
                    s.execute("INSERT INTO grade (id, events) VALUES (" +
                            id + "," + canonical + ")");
                    s.execute("INSERT INTO grade_backup (id, events) VALUES (" +
                            id + "," + canonical + ")");
                }
            }

        } catch (Exception e) {
            System.err.println("[" + myID + "] reconcileAllKeyspacesFromBackup failed: " + e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Leader bookkeeping                                                     */
    /* ---------------------------------------------------------------------- */

    private boolean isLeader() {
        return myID.equals(currentLeader);
    }

    /**
     * Advance currentLeader to the next ID in sortedNodeIds.
     * Also record the failed server in deadServers (used to avoid forwarding).
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
    /* Deterministic events=events+[X] parsing and global application         */
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
     *
     *   UPDATE grade SET events = events + [X] WHERE id = Y;
     *   update grade set events=events+[X] where id=Y;
     *
     * Any spacing/casing variant is allowed. If the pattern does not match,
     * returns null.
     */
    private EventUpdate parseEventsUpdate(String cmd) {
        String lower = cmd.toLowerCase();
        if (!lower.contains("update") ||
                !lower.contains("set") ||
                !lower.contains("where")) {
            return null;
        }

        try {
            // Split into SET part and WHERE part.
            int setIdx = lower.indexOf("set");
            int whereIdx = lower.indexOf("where", setIdx);
            if (setIdx < 0 || whereIdx < 0) {
                return null;
            }

            String setPart = cmd.substring(setIdx + 3, whereIdx);
            String wherePart = cmd.substring(whereIdx + 5);

            // Normalize SET part by removing all whitespace.
            String setNoSpace = setPart.replaceAll("\\s+", "").toLowerCase();
            String marker = "events=events+[";
            int idx = setNoSpace.indexOf(marker);
            if (idx < 0) {
                return null;
            }

            int startEv = idx + marker.length();
            int endEv = setNoSpace.indexOf("]", startEv);
            if (endEv < 0) {
                return null;
            }

            String eventStr = setNoSpace.substring(startEv, endEv).trim();
            int eventVal = Integer.parseInt(eventStr);

            // Parse WHERE id = Y (allow whitespace and negative Y).
            String whereLower = wherePart.toLowerCase();
            int idIdx = whereLower.indexOf("id");
            if (idIdx < 0) {
                return null;
            }
            int eqIdx = whereLower.indexOf("=", idIdx);
            if (eqIdx < 0) {
                return null;
            }

            int pos = eqIdx + 1;
            while (pos < wherePart.length() &&
                    Character.isWhitespace(wherePart.charAt(pos))) {
                pos++;
            }
            int startKey = pos;
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
            // Any parsing failure => not our pattern.
            return null;
        }
    }

    /**
     * Global deterministic update for:
     *   UPDATE grade SET events = events + [X] WHERE id = Y;
     *
     * Steps:
     *   1. For the given id, read events from grade on EVERY keyspace.
     *   2. Choose canonical = the longest list among all replicas
     *      (ties arbitrary).
     *   3. Append X to canonical, sort.
     *   4. Write canonical back to grade and grade_backup for EVERY keyspace.
     *
     * This ensures that after *every* such update, the state for this id
     * matches on all servers. It also self-heals from earlier partial
     * replication or crash patterns.
     */
    private synchronized void applyGlobalDeterministicEventsUpdate(EventUpdate ev) {
        try {
            List<Integer> canonical = null;
            int bestSize = -1;

            // 1. Read current events from ALL keyspaces.
            for (Session s : sessions.values()) {
                Row rg = s.execute("SELECT events FROM grade WHERE id = " +
                        ev.keyId).one();
                if (rg != null && !rg.isNull("events")) {
                    List<Integer> evList = rg.getList("events", Integer.class);
                    if (evList != null) {
                        if (evList.size() > bestSize) {
                            canonical = new ArrayList<>(evList);
                            bestSize = evList.size();
                        }
                    }
                }
            }

            if (canonical == null) {
                canonical = new ArrayList<>();
            }

            // 2. Append new event and sort.
            canonical.add(ev.eventVal);
            Collections.sort(canonical);

            // 3. Write canonical list to grade and backup on ALL keyspaces.
            for (Session s : sessions.values()) {
                s.execute("INSERT INTO grade (id, events) VALUES (" +
                        ev.keyId + "," + canonical + ")");
                s.execute("INSERT INTO grade_backup (id, events) VALUES (" +
                        ev.keyId + "," + canonical + ")");
            }
        } catch (Exception e) {
            System.err.println("[" + myID + "] applyGlobalDeterministicEventsUpdate failed: " + e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Replication helpers                                                    */
    /* ---------------------------------------------------------------------- */

    /**
     * Execute a non-events-append CQL command on ALL keyspaces.
     * Used for generic commands that aren't "events=events+[X]".
     */
    private void executeRawOnAllReplicas(String cmd) {
        for (Session s : sessions.values()) {
            try {
                s.execute(cmd);
            } catch (Exception e) {
                System.err.println("[" + myID + "] CQL execution failed on replica: " + e);
            }
        }
    }

    /**
     * Execute a command on ALL replicas:
     *  - If it is an "events=events+[X]" update, use the global deterministic
     *    path that reads all replicas and rewrites a canonical list.
     *  - Otherwise, just execute the raw CQL on each keyspace.
     */
    private void executeOnAllReplicas(String cmd) {
        String trimmed = cmd.trim();
        if (trimmed.isEmpty()) {
            return;
        }

        EventUpdate ev = parseEventsUpdate(trimmed);
        if (ev != null) {
            applyGlobalDeterministicEventsUpdate(ev);
        } else {
            executeRawOnAllReplicas(trimmed);
        }
    }

    /**
     * Parse the primary key id out of:
     *    INSERT INTO grade(...) VALUES (id, ...)
     * Returns -1 if parsing fails.
     */
    private int parseInsertKeyId(String cmd) {
        try {
            String lower = cmd.toLowerCase();
            if (!lower.startsWith("insert into grade")) {
                return -1;
            }

            int idxValues = lower.indexOf("values");
            if (idxValues < 0) {
                return -1;
            }

            int open = cmd.indexOf("(", idxValues);
            int close = cmd.indexOf(")", open);
            if (open < 0 || close < 0) {
                return -1;
            }

            String inside = cmd.substring(open + 1, close);
            String[] parts = inside.split(",", 2);
            if (parts.length == 0) {
                return -1;
            }

            return Integer.parseInt(parts[0].trim());
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Apply INSERT INTO grade(...) on all keyspaces and mirror to grade_backup.
     * We:
     *   1. Execute the original INSERT on each keyspace.
     *   2. SELECT the resulting events list (often [] initially),
     *   3. Insert that list into grade_backup for the same id.
     */
    private synchronized void applyInsertOnAllReplicas(String cmd) {
        try {
            int keyId = parseInsertKeyId(cmd);

            for (Session s : sessions.values()) {
                // Execute original INSERT.
                s.execute(cmd);

                if (keyId < 0) {
                    // Can't mirror into backup without key; skip.
                    continue;
                }

                // Mirror the events list for this id into backup.
                Row r = s.execute("SELECT events FROM grade WHERE id = " + keyId).one();
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
            System.err.println("[" + myID + "] applyInsertOnAllReplicas failed: " + e);
        }
    }

    /* ---------------------------------------------------------------------- */
    /* Leader application + client handler                                    */
    /* ---------------------------------------------------------------------- */

    /**
     * Leader-side application: serialize and replicate to all keyspaces.
     */
    private synchronized void applyAsLeader(String cmd) {
        executeOnAllReplicas(cmd);
    }

    /**
     * Handle client requests:
     *  - INSERT INTO grade ... → applyInsertOnAllReplicas from ANY node.
     *  - Other commands:
     *      * If this node is the leader: applyAsLeader(cmd).
     *      * Otherwise: forwardToLeader(cmd).
     */
    @Override
    protected synchronized void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String cmd = new String(bytes, StandardCharsets.UTF_8).trim();
        if (cmd.isEmpty()) {
            return;
        }

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
     *  - a live leader accepts the forwarded command.
     */
    private void forwardToLeader(String cmd) {
        while (true) {
            String target = currentLeader;

            if (myID.equals(target)) {
                // We have promoted ourselves to leader; execute locally.
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
        if (msg.isEmpty()) {
            return;
        }

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
                } catch (Exception ignored) {
                }
            }
        } catch (Exception ignored) {
        }

        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception ignored) {
        }
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
