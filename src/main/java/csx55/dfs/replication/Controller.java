package csx55.dfs.replication;

import csx55.dfs.config.ControllerConfig;
import csx55.dfs.domain.*;
import csx55.dfs.payload.ChunkLocationPayload;
import csx55.dfs.payload.MajorHeartBeat;
import csx55.dfs.payload.Message;
import csx55.dfs.payload.MinorHeartBeat;
import csx55.dfs.transport.TCPConnection;
import csx55.dfs.transport.TCPServerThread;
import csx55.dfs.utils.ChunkServerRanker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Controller implements Node {

    private static String FAULT_TOLERANCE_MODE = FaultToleranceMode.REPLICATION.getMode(); //if nothing, then default mode is Replication
    /***
     * These are the maps maintained in the controller to track
     * various distributed statistics about the chunkServers
     * and the chunks stored across the cluster
     * **********
     * - chunkServerInfoMap -> high level map between node to the high level chunkServer info
     *   such as node IP, node port, available space, etc
     * **********
     * - chunkMetaDataMap -> map between filename to the list of chunk
     *  metadata gathered from across the chunkServers in the cluster
     *  ************
     *  - chunkStorageMap -> map between chunk to the list of chunk severs
     *  where these chunk replicas are stored. Useful for retrieval, fault tolerance
     *  ************
     *  - chunkServerAvailableSpaceMap -> map between chunkServer to
     *  the available space it contains. this will make it easier to
     *  rank ChunkServers based on space and then inform the client
     *  to send their chunks based on this ranking
     * **********
     *  - lastHeartbeatReceived -> map between chunkServer and the timestamp
     *  when we received the last heartbeat (major or minor) from it. this will
     *  be compared against the HEARTBEAT_TIMEOUT interval to detect disconnected
     *  chunkServers and initiate fault tolerance / recovery procedures
     */
    private static Map <String, List <String>> chunkServerInfoMap; //list of chunks a chunkServer maintains

    private static Map <String, List <String>> chunkServerShardsInfoMap;

    private static Map <String, List <ChunkMetaData>> chunkMetaDataMap;

    private static Map <String, List <String>> chunkStorageMap;

    private static Map <String, List <String>> shardStorageMap;

    private static Map <String, Long> chunkServerAvailableSpaceMap;

    private static Map<String, Long> lastHeartbeatReceived;

    static {
        chunkServerInfoMap = new ConcurrentHashMap<>();
        chunkServerShardsInfoMap = new ConcurrentHashMap<>();
        chunkMetaDataMap = new ConcurrentHashMap<>();
        chunkStorageMap = new ConcurrentHashMap<>();
        shardStorageMap = new ConcurrentHashMap<>();
        chunkServerAvailableSpaceMap = new ConcurrentHashMap<>();
        lastHeartbeatReceived = new ConcurrentHashMap<>();
    }

    /***
     * Set of chunk servers that have sent heartbeat to controller
     * leading to its "discovery"
     */
    private static Set <String> discoveredChunkServers = new HashSet<>();

    private static Map <String, TCPConnection> tcpCache = new HashMap<>();
    private static final Logger logger = Logger.getLogger(Controller.class.getName());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Controller instance = new Controller();
    public static Controller getInstance() {
        return instance;
    }
    private static ScheduledExecutorService heartbeatChecker = Executors.newSingleThreadScheduledExecutor();
    public static void main (String [] args) {
        if (args.length == 2) {
            FAULT_TOLERANCE_MODE = args[1];
            FAULT_TOLERANCE_MODE = FaultToleranceMode.RS.getMode();
        }
        System.out.println(FAULT_TOLERANCE_MODE);

        int controllerPort = args.length >= 1 ? Integer.parseInt(args[0]) : 12345;
        try  {
            ServerSocket serverSocket = new ServerSocket(controllerPort);
            System.out.println("Server listening on port " + controllerPort + "...");
            Controller controller = Controller.getInstance();
            (new Thread(new TCPServerThread(controller, serverSocket))).start();
            setupHeartbeatChecker();
        } catch (IOException e) {
            logger.severe("Error in the serverSocket communication channel" + e);
        }
    }

    public synchronized void receiveMajorHeartBeat (MajorHeartBeat majorHb) {
        discoveredChunkServers.add(majorHb.getHeartBeatOrigin());
        chunkServerInfoMap.put(majorHb.getHeartBeatOrigin(), majorHb.getAllChunkFiles());
        chunkServerShardsInfoMap.put(majorHb.getHeartBeatOrigin(), majorHb.getAllShards());

        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Received major heart beat at: {0}", formatter.format(now));
        lastHeartbeatReceived.put(majorHb.getHeartBeatOrigin(), System.currentTimeMillis());
        System.out.println(majorHb.toString());

        if (chunkServerAvailableSpaceMap.containsKey(majorHb.getHeartBeatOrigin())) {
            chunkServerAvailableSpaceMap.replace(majorHb.getHeartBeatOrigin(), majorHb.getFreeSpaceAvailable());
        }
        else {
            chunkServerAvailableSpaceMap.put(majorHb.getHeartBeatOrigin(), majorHb.getFreeSpaceAvailable());
        }
        //printSpaceAvailableMapElement(chunkServerAvailableSpaceMap);
//        List<List<String>> serversForChunks = ChunkServerRanker.rankChunkServersForChunks(ControllerConfig.NUM_CHUNKS, chunkServerAvailableSpaceMap);
//        for (int i = 0; i < serversForChunks.size(); i++) {
//            System.out.println("Chunk " + (i + 1) + " servers: " + serversForChunks.get(i));
//        }
    }

    public synchronized void receiveMinorHeartBeat(MinorHeartBeat minorHb) {
        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Received minor heart beat at: {0}", formatter.format(now));
        lastHeartbeatReceived.put(minorHb.getHeartBeatOrigin(), System.currentTimeMillis());
        System.out.println(minorHb.toString());
        if (chunkServerInfoMap.containsKey(minorHb.getHeartBeatOrigin())) {
            chunkServerInfoMap.get(minorHb.getHeartBeatOrigin()).addAll(minorHb.getNewChunkFiles());
        }
        else {
            chunkServerInfoMap.put(minorHb.getHeartBeatOrigin(), minorHb.getNewChunkFiles());
        }
        if (chunkServerShardsInfoMap.containsKey(minorHb.getHeartBeatOrigin())) {
            chunkServerShardsInfoMap.get(minorHb.getHeartBeatOrigin()).addAll(minorHb.getNewShards());
        }
        else {
            chunkServerShardsInfoMap.put(minorHb.getHeartBeatOrigin(), minorHb.getNewShards());
        }

        if (chunkServerAvailableSpaceMap.containsKey(minorHb.getHeartBeatOrigin())) {
            chunkServerAvailableSpaceMap.replace(minorHb.getHeartBeatOrigin(), minorHb.getFreeSpaceAvailable());
        }
        else {
            chunkServerAvailableSpaceMap.put(minorHb.getHeartBeatOrigin(), minorHb.getFreeSpaceAvailable());
        }
        //printSpaceAvailableMapElement(chunkServerAvailableSpaceMap);
//        List<List<String>> serversForChunks = ChunkServerRanker.rankChunkServersForChunks(ControllerConfig.NUM_CHUNKS, chunkServerAvailableSpaceMap);
//        for (int i = 0; i < serversForChunks.size(); i++) {
//            System.out.println("Chunk " + (i + 1) + " servers: " + serversForChunks.get(i));
//        }
    }

    private static void setupHeartbeatChecker() {
        heartbeatChecker.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastHeartbeatReceived.forEach((key, lastTime) -> {
                if ((now - lastTime) > ControllerConfig.HEARTBEAT_TIMEOUT) {
                    logger.warning("Heartbeat timeout for server: " + key);
                    // Here you might also want to try reconnecting or marking the server as down.
                    //TODO fault tolerance. File transfers via data plane
                    /***
                     * First remove the down server from our storage space maps
                     */
                    chunkServerAvailableSpaceMap.remove(key);

                    /***
                     * Removing down server from our chunk to replica server map
                     */
                    chunkStorageMap.forEach((chunkPath, nodeList) -> {
                        // Filter out the down node from each list
                        List<String> updatedList = nodeList.stream()
                                .filter(node -> !node.equals(key))
                                .collect(Collectors.toList());
                        // Update the map with the new list
                        chunkStorageMap.put(chunkPath, updatedList);
                    });

                    // Optionally, remove any entries that now have empty lists if that makes sense for your logic
                    chunkStorageMap.entrySet().removeIf(entry -> entry.getValue().isEmpty());
                    //remove the disconnected server so that we don't end up selecting it as a backup
                    discoveredChunkServers.removeIf(i -> i.equals(key));

                    /***
                     * Now that the maps are reconstructed, let us start recovery procedure
                     * to restore the number of replicas
                     * First let us search for the chunks the lost node was responsible for
                     */
                    List <String> lostChunks = chunkServerInfoMap.get(key);
                    //now let us figure out the proper nodes to store each of these chunks
                    //NOTE: we cannot store replica of same node on one machine
                    for (String chunk : lostChunks) {
                        List<String> replicaNodes =
                                chunkStorageMap.isEmpty()
                                        || !chunkStorageMap.containsKey(chunk)
                                ? new ArrayList<>() : chunkStorageMap.get(chunk);

                        //well this happens if the user uploaded nothing
                        //and all existing chunks are from a previous time
                        //in that case, use the live chunkServerInfoMap
                        if (chunkStorageMap.isEmpty() || !chunkStorageMap.containsKey(chunk)) {
                            for (Map.Entry<String, List<String>> entry : chunkServerInfoMap.entrySet()) {
                                // Check if the node's list of chunks contains the current chunk
                                if (entry.getValue().contains(chunk)) {
                                    // If it contains, add the node's name to the replicaNodes list
                                    replicaNodes.add(entry.getKey());
                                }
                            }
                        }

                        // Ensure replicaNodes is not null to avoid NullPointerException
                        if (replicaNodes != null) {
                            //the new backup server shouldn't have a replica for the chunk already
                            //and it shouldn't be the node that just passed
                            Optional<String> server = discoveredChunkServers.stream()
                                    .filter(el -> !replicaNodes.contains(el) &&  !el.equals(key))
                                    .findFirst();

                            /***
                             * Let's initiate recovery at newly appointed backup node
                             */
                            if (server.isPresent()) {
                                //remove the dead replica node if it's present in the replica list
                                List <String> filteredReplicas = replicaNodes.stream()
                                        .filter(i -> !i.equals(key)).collect(Collectors.toList());
                                String selectedServer = server.get();

                                //if no replicas other than
                                //the failed node, then recovery is not possible
                                if (filteredReplicas.isEmpty()) {
                                    System.out.println("Recovery not possible for chunk "+ chunk + " due to no available live replicas");
                                    continue;
                                }
                                // Do something with selectedServer, such as initiate a repair operation
                                System.out.println("Selected server for chunk " + chunk + ": " + selectedServer);

                                TCPConnection connectionToBackup = getTCPConnection(tcpCache,
                                        selectedServer.split(":")[0],
                                        Integer.parseInt(selectedServer.split(":")[1]));

                                Message recoveryMsg = new Message(Protocol.RECOVER_REPLICA, chunk, filteredReplicas);
                                try {
                                    connectionToBackup.getSenderThread().sendData(recoveryMsg);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException(e);
                                }
                                //add newly appointed replica holder to the storage Map
                                if (chunkStorageMap.get(chunk) != null) {
                                    chunkStorageMap.get(chunk).add(selectedServer);
                                }
                                else {
                                    //chunkStorageMap empty as in no uploads
                                    //only archived chunk files scenario
                                    List <String> location = new ArrayList<>();
                                    location.add(selectedServer);
                                    chunkStorageMap.put(chunk, location);
                                }

                            } else {
                                System.out.println("No available servers found for chunk " + chunk);
                            }
                        } else {
                            System.out.println("No replica nodes found for chunk " + chunk);
                        }
                    }
                    //remove the faulty node entry since recovery is done
                    lastHeartbeatReceived.remove(key);

                }
            });
        }, 5, 30, TimeUnit.SECONDS); // Check every minute
    }

    private void printSpaceAvailableMapElement (Map <String, Long> chunkServerAvailableSpaceMap) {
        for (Map.Entry<String, Long> entry: chunkServerAvailableSpaceMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }


    /***
     * Message acknowledgments when Controller is the receiver
     */
    public void generateChunkServerRankingForClient (TCPConnection conn, List <Long> chunkSizes, List <String> chunkNames) {
        List<List<String>> serversForChunks = FAULT_TOLERANCE_MODE != null &&
                FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())
                ? ChunkServerRanker.rankChunkServersForChunks(chunkSizes, chunkServerAvailableSpaceMap, FaultToleranceMode.RS)
                : ChunkServerRanker.rankChunkServersForChunks(chunkSizes, chunkServerAvailableSpaceMap);

        /***
         * Fill in the local chunk storage map
         * In the future, when client requests download for a file
         * we can use this map to assemble the chunks and then send
         * the whole file to client.
         */
        for (int i = 0; i < chunkNames.size(); i++) {
            String chunkName = chunkNames.get(i);
            //replica servers where this chunk and its replicas are stored
            List <String> replicaServers = serversForChunks.get(i);
            chunkStorageMap.put(chunkName, replicaServers);
        }

        try {
            Message rankingResponse = new Message(Protocol.CHUNK_SERVER_RANKING_RESPONSE, serversForChunks);
            conn.getSenderThread().sendData(rankingResponse);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * Send replica location for a file when requested
     * Client will request this during download
     * ChunkServer will request this during recovery from tampering
     */

    public void sendReplicaLocations(TCPConnection conn, Message msg) {
        //file for which replica request was requested
        String replicaInfoRequestedFor = (String) msg.getPayload();

        Map<String, List<String>> filteredChunkLocations = new HashMap<>();

        // Iterate over the entries in the chunk storage map
        for (Map.Entry<String, List<String>> entry : chunkStorageMap.entrySet()) {
            String chunkKey = entry.getKey();

            // Check if the chunk key starts with the file description plus "_chunk"
            if (chunkKey.startsWith(replicaInfoRequestedFor + "_chunk")
            || chunkKey.equals(replicaInfoRequestedFor)) { //the chunkServer will send exact chunk info in the request, so adding OR to handle that
                // If it matches, put the chunk and its server list into the new map
                filteredChunkLocations.put(chunkKey, entry.getValue());
            }
        }

        // Create a ChunkLocationPayload object with the filtered map
        ChunkLocationPayload payload = new ChunkLocationPayload(filteredChunkLocations);

        try {
            conn.getSenderThread().sendObject(payload);
        } catch (IOException | InterruptedException e) {
            System.err.println("Failed to send chunk location data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /***
     * The chunkServer contacts us saying that the chunk is tampered.
     * We will now return the list of potentially pure replicas to the origin node
     * 'Potentially', because the other nodes might have reported corruption as well
     *
     * @param conn
     * @param msg
     */
    public void sendPristineReplicaLocation (TCPConnection conn, Message msg) {
        String originNodeWithCorruptedSlice = msg.getAdditionalPayload().get(0);
        String corruptedChunkPath = (String) msg.getPayload();

        // Retrieve the list of nodes that store this chunk
        List<String> nodesWithReplica = chunkStorageMap.get(corruptedChunkPath);

        if (nodesWithReplica == null) {
            System.out.println("No nodes found for the given chunk path");
            return;
        }

        // Filter out the node with the corrupted slice
        List<String> nodesWithPristineReplica = nodesWithReplica.stream()
                .filter(node -> !node.equals(originNodeWithCorruptedSlice))
                .collect(Collectors.toList());

        // Example: Send the list of pristine replicas back to the requester
        // You might need to serialize the list or handle it according to your application's needs
        try {
            conn.getSenderThread().sendData(new Message(Protocol.PRISTINE_CHUNK_LOCATION_RESPONSE, "",
                    nodesWithPristineReplica));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Alternatively, handle the list of nodes as needed by your application logic
        System.out.println("Pristine replicas are located at: " + nodesWithPristineReplica);

    }


    /***
     * TCP Cache
     */
    public static TCPConnection getTCPConnection(Map<String, TCPConnection> tcpCache, String chunkServerIp, int chunkServerPort) {

        TCPConnection conn = null;
        if (tcpCache.containsKey(chunkServerIp+ ":" + chunkServerPort)) {
            conn = tcpCache.get(chunkServerIp+ ":" + chunkServerPort);
        }
        else {
            try {
                Socket clientSocket = new Socket(chunkServerIp, chunkServerPort);
                conn = new TCPConnection(Controller.getInstance(), clientSocket);
                tcpCache.put(chunkServerIp + ":" + chunkServerPort, conn);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        if (!conn.isStarted()) {
            conn.startConnection();
        }
        return conn;
    }

}
