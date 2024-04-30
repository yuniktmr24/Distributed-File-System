package csx55.dfs.replication;

import csx55.dfs.config.ChunkServerConfig;
import csx55.dfs.domain.ChunkMetaData;
import csx55.dfs.domain.FaultToleranceMode;
import csx55.dfs.domain.Node;
import csx55.dfs.domain.Protocol;
import csx55.dfs.payload.*;
import csx55.dfs.transport.TCPConnection;
import csx55.dfs.transport.TCPServerThread;
import csx55.dfs.utils.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ChunkServer implements Node {
    private static String FAULT_TOLERANCE_MODE = FaultToleranceMode.REPLICATION.getMode();;
    private static final Logger logger = Logger.getLogger(ChunkServer.class.getName());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private String nodeIp;

    private Integer nodePort;

    private String descriptor;

    private TCPConnection controllerConnection;

    private final AtomicLong lastMajorHeartbeat = new AtomicLong(0);

    private int salt;

    private Map<String, ChunkMetaData> chunkMetaDataMap = new ConcurrentHashMap<>();

    private Map<String, List<String>> initialChecksums = new ConcurrentHashMap<>();

    private String fileStorageDirectory;

    private Map <String, TCPConnection> tcpCache = new HashMap<>();

    private List <String> nodesWithPureReplica = new ArrayList<>();

    private CountDownLatch waitForNodeInfoAboutPureReplicas;

    private CountDownLatch checksumsVerified;

    /***
     * Chunks which were part of the previous minor/major heartbeat sync
     */
    private List <String> previouslySyncedChunks = new ArrayList<>();
    private List <String> previouslySyncedShards = new ArrayList<>();

    private List <String> chunksWithShards = new ArrayList<>();

    private Map <String, ShardWrapper> shardWrapperMap = new ConcurrentHashMap<>();

    private CountDownLatch shardsReceived;

    public static void main(String[] args) {
        //REED SOLOMON RECOVERY MODE. Debug = args=4
        if (args.length == 4) {
            FAULT_TOLERANCE_MODE = args[2];
            //assuming Reed-Solomon mode is the only mode specified via cmd line
            FAULT_TOLERANCE_MODE = FaultToleranceMode.RS.getMode();
        }
        //PROD - MODE
        if (args.length == 3) {
            FAULT_TOLERANCE_MODE = args[2];
            //assuming Reed-Solomon mode is the only mode specified via cmd line
            FAULT_TOLERANCE_MODE = FaultToleranceMode.RS.getMode();
        }
        try (Socket socketToController = new Socket(args[0], Integer.parseInt(args[1]));
         //try (Socket socketToController = new Socket("localhost", 12345);
             ServerSocket chunkServerSocket = new ServerSocket(0);
        ) {
            ChunkServer chunkServer = new ChunkServer();

            /* DEBUG helpers*/
             if (args.length == 1 && ChunkServerConfig.DEBUG_MODE) {
                 chunkServer.salt = Integer.parseInt(args[0]);
                 System.out.println("The salt value for debug "+ chunkServer.salt);
             }

             if (args.length == 4 && ChunkServerConfig.DEBUG_MODE) {
                 chunkServer.salt = Integer.parseInt(args[3]);
                 System.out.println("The salt value for debug "+ chunkServer.salt);
             }

            chunkServer.setServiceDiscovery(InetAddress.getLocalHost().getHostAddress(), chunkServerSocket.getLocalPort());



             /***
              * Set up controller connection
              */
            chunkServer.setAndStartControllerConnection(new TCPConnection(chunkServer, socketToController));

            Thread chunkServerThread = new Thread(new TCPServerThread(chunkServer, chunkServerSocket));
            chunkServerThread.start();

             /***
              * Setup the heartbeat transmission schedule
              * Setup the metadata - last modified, incrementVersion update schedule
              * Setup the checksum integrity checker schedule
              */
            chunkServer.initiateHeartBeat();
            chunkServer.startScheduledChunkMetaDataCheck();
            chunkServer.startScheduledChunkChecksumCheck();

            while (true) {

            }
        } catch (IOException e) {
            logger.severe("Error in main thread" + e.getMessage());
        }
    }

    private void setServiceDiscovery(String ip, Integer port) {
        this.nodeIp = ip;
        this.nodePort = port;
        this.descriptor = ip + ":" + port;
        this.fileStorageDirectory = !ChunkServerConfig.DEBUG_MODE ?
                ChunkServerConfig.CHUNK_STORAGE_ROOT_DIRECTORY
                : ChunkServerConfig.CHUNK_STORAGE_ROOT_DIRECTORY + "/" + ip + "-" +  salt;
    }


    private void sendMinorHeartBeat() {
        long currentTime = System.currentTimeMillis();
        long lastMajor = lastMajorHeartbeat.get();

        // Check if it's been 2 minutes since the last major heartbeat
        if (currentTime - lastMajor >= TimeUnit.MINUTES.toMillis(2)) {
            // If yes, do not send a minor heartbeat. A major heartbeat will be sent instead.
            return;
        }
        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Sending Minor Heartbeat at: {0}", formatter.format(now));
        MinorHeartBeat minorHB = new MinorHeartBeat(getDescriptor(),
                ChunkServerConfig.DEBUG_MODE ? FileUtils.getNumberOfChunks(nodeIp, salt) : FileUtils.getNumberOfChunks(),
                ChunkServerConfig.DEBUG_MODE ? FileUtils.getAvailableStorage(nodeIp, salt) : FileUtils.getAvailableStorage());

        List <Path> chunkFilePaths = ChunkServerConfig.DEBUG_MODE ? FileUtils.getChunkFilesWithExtension(nodeIp, salt) : FileUtils.getChunkFilesWithExtension("");
        //fully qualified names - including path info
        List <String> currentChunkFileNames = chunkFilePaths.stream()
                .map(Path::toString)
                .map(el -> el.replace(this.fileStorageDirectory, ""))
                .collect(Collectors.toList());

        Set<String> previouslySyncedSet = new HashSet<>(previouslySyncedChunks);
        Set<String> currentFileSet = new HashSet<>(currentChunkFileNames);

        currentFileSet.removeAll(previouslySyncedSet);
        System.out.println("New chunks since last sync: " + currentFileSet);

        List<String> newChunks = new ArrayList<>(currentFileSet);

        minorHB.setNewChunkFiles(new ArrayList<>(currentFileSet));
        // Add new chunks to the previously synced chunks list
        previouslySyncedChunks.addAll(newChunks);


        /***
         * Do the same as above with shard files
         */
        if (FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())) {
            List<Path> shardPaths = ChunkServerConfig.DEBUG_MODE ? FileUtils.getShardsWithExtension(nodeIp, salt) : FileUtils.getShardsWithExtension("");
            //fully qualified names - including path info
            List<String> currentShardNames = shardPaths.stream()
                    .map(Path::toString)
                    .map(el -> el.replace(this.fileStorageDirectory, ""))
                    .collect(Collectors.toList());

            Set<String> previouslySyncedShardsSet = new HashSet<>(previouslySyncedShards);
            Set<String> currentShardSet = new HashSet<>(currentShardNames);

            currentShardSet.removeAll(previouslySyncedShardsSet);
            System.out.println("New shards since last sync: " + currentShardSet);

            List<String> newShards = new ArrayList<>(currentShardSet);

            minorHB.setNewShards(new ArrayList<>(currentShardSet));
            // Add new shards to the previously synced shards list
            previouslySyncedShards.addAll(newShards);
        }
        try {
            this.controllerConnection.getSenderThread().sendData(minorHB);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if (FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())) {
            for (Path chunkPath : chunkFilePaths) {
                String chunkName = chunkPath.toString().replace(this.fileStorageDirectory, "");

                /***
                 * If chunk doesn't have shards generated then, well let us generate the shards. Only in Reed solomon
                 */
                List<ShardWrapper> generatedShards = new ArrayList<>();
                if (!chunksWithShards.contains(chunkName) && !shardWrapperMap.keySet().stream() // Create a stream of the keys
                        .anyMatch(key -> key.contains(chunkName))) {
                    generatedShards = generateShards(chunkPath);

                    List<String> generatedShardNames = generatedShards.stream().map(ShardWrapper::getShardName)
                            .collect(Collectors.toList());

                    for (ShardWrapper shard : generatedShards) {
                        shardWrapperMap.put(shard.getShardName(), shard);
                    }

                    /***
                     * After generating the shards, let us contact the controller node to get
                     * the shard storage nodes locations
                     */
                    Message shardStorageLocationReqMsg = new Message(Protocol.SHARD_STORAGE_RANKING_REQUEST, this.descriptor
                            , generatedShardNames);

                    try {
                        this.controllerConnection.getSenderThread().sendData(shardStorageLocationReqMsg);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }

    /***
     * Generate shards for a given file given its path
     * @param path
     */
    private List <ShardWrapper> generateShards (Path path) {
        List <ShardWrapper> shards = new ArrayList<>();
        try {
            byte[] data = Files.readAllBytes(path);
            String chunkName = path.toString().replace(this.fileStorageDirectory, "");
            String chunkFilePath = path.toString();

            ChunkWrapper wrapper = new ChunkWrapper(data, chunkName, chunkFilePath);
            shards = ReedSolomonFileEncoder.encodeShards(wrapper);

            chunksWithShards.add(chunkName);
        } catch (IOException e) {
            System.err.println("Error reading file: " + path);
            e.printStackTrace();
            // Depending on your requirements, you may want to handle the error differently
            // For example, you could return null or throw a custom exception
        }
        return shards;
    }

    private void sendMajorHeartBeat() {
        lastMajorHeartbeat.set(System.currentTimeMillis());
        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Sending Major Heartbeat at: {0}", formatter.format(now));
        MajorHeartBeat majorHB = new MajorHeartBeat(getDescriptor(),
                ChunkServerConfig.DEBUG_MODE ? FileUtils.getNumberOfChunks(nodeIp, salt) : FileUtils.getNumberOfChunks(),
                ChunkServerConfig.DEBUG_MODE ? FileUtils.getAvailableStorage(nodeIp, salt) : FileUtils.getAvailableStorage());

        List <Path> chunkFilePaths = ChunkServerConfig.DEBUG_MODE ? FileUtils.getChunkFilesWithExtension(nodeIp, salt) : FileUtils.getChunkFilesWithExtension("");
        //fully qualified names - including path info
        List <String> chunkFileNames = chunkFilePaths.stream()
                .map(Path::toString)
                .map(el -> el.replace(this.fileStorageDirectory, ""))
                .collect(Collectors.toList());

        if (FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())) {
            List<Path> shardFilePaths = ChunkServerConfig.DEBUG_MODE ? FileUtils.getShardsWithExtension(nodeIp, salt) : FileUtils.getShardsWithExtension("");
            //fully qualified names - including path info
            List<String> shardNames = shardFilePaths.stream()
                    .map(Path::toString)
                    .map(el -> el.replace(this.fileStorageDirectory, ""))
                    .collect(Collectors.toList());
            previouslySyncedShards = shardNames;
            majorHB.setAllShards(shardNames);
        }

        previouslySyncedChunks = chunkFileNames;
        majorHB.setAllChunkFiles(chunkFileNames);

        try {
            this.controllerConnection.getSenderThread().sendData(majorHB);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initiateHeartBeat () {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
        executor.scheduleAtFixedRate(this::sendMinorHeartBeat, 0, 15, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(this::sendMajorHeartBeat, 0, 120, TimeUnit.SECONDS);
    }

    private void startScheduledChunkMetaDataCheck() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::checkAndUpdateMetadata, 5, 25, TimeUnit.SECONDS); // Runs every hour
    }

    private void startScheduledChunkChecksumCheck() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable checksumVerificationTask = this::verifyCheckSumsAndInitiateRepair;

        // Schedule the task to run every 15 seconds
        scheduler.scheduleAtFixedRate(checksumVerificationTask, 5, 15, TimeUnit.SECONDS);
    }

    /***
     * Sub-Routine to verify checksums and contact controller for pure replicas if violations
     * are detected
     */
    private void verifyCheckSumsAndInitiateRepair () {
        Map<String, List<String>> currentCheckSums = FileChecksumCalculator.getChecksumMapForChunkInDirectory(this.fileStorageDirectory);
        System.out.println("Scheduled checksum calculation completed. Total files processed: " + currentCheckSums.size());
        Map <String, List <Integer>> checksumViolationMap = verifyChecksums(currentCheckSums);

        for (Map.Entry<String, List<Integer>> chunkChecksum: checksumViolationMap.entrySet()) {
            List<Integer> checksumViolationSlices = chunkChecksum.getValue();
            if (!checksumViolationSlices.isEmpty()) {
                /***
                 *    we will need to perform erasure coding
                 *    either via @replication or @reed-solomon.
                 *
                 *    first tell controller about the violation
                 *    tell which chunk is violated e.g: data.txt_chunk1
                 *
                 *    A] FOR REPLICATION:
                 *    the controller will then return a list of other nodes
                 *    holding the replica for this chunk
                 *
                 *    this node will then contact the other nodes and try to perform
                 *    restoration. But for that, when the other node receives the
                 *    ERASURE_VIA_REPLICATION_REQUEST, it will need to verify that
                 *    its checksumViolationMap doesn't have a non-empty list for
                 *    the given chunk. Else, it will return an error message in the
                 *    payload
                 *
                 */
                //TODO
                waitForNodeInfoAboutPureReplicas = new CountDownLatch(1);
                //Before RepairRequest. Let us talk to the controller, requesting node
                //with proper replica.
                Message requestForPristineChunkLocation = new Message(Protocol.PRISTINE_CHUNK_LOCATION_REQUEST,
                        chunkChecksum.getKey(), Collections.singletonList(this.descriptor));
                try {
                    this.controllerConnection.getSenderThread().sendData(requestForPristineChunkLocation);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                /***
                 * Let's wait for the controller to reply with list of pure replicas
                 */
                try {
                    System.out.println("Waiting for controller to reply with replica locations for repair");
                    waitForNodeInfoAboutPureReplicas.await();
                    System.out.println("Controller replied with replica locations");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ChunkRepairRequest chunkRepair = new ChunkRepairRequest(this.descriptor,
                        chunkChecksum.getKey(), checksumViolationSlices);
                System.out.println("Tampered checksum description ");
                System.out.println(chunkRepair.toString());

                /***
                 * Pick one node from the list of nodes with pure replicas
                 */
                String selectedNodeWithPureReplica = this.nodesWithPureReplica.get(0);

                String selectedNodeWithPureReplicaIP = selectedNodeWithPureReplica.split(":")[0];
                int selectedNodeWithPureReplicaPort = Integer.parseInt(selectedNodeWithPureReplica.split(":")[1]);

                TCPConnection conn = getTCPConnection(tcpCache, selectedNodeWithPureReplicaIP, selectedNodeWithPureReplicaPort);

                try {
                    conn.getSenderThread().sendData(chunkRepair);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            //no errors. so okay to release latch
            else {
                if (checksumsVerified != null) {
                    checksumsVerified.countDown();
                }
            }
        }
    }

    public Map <String, List <Integer>> verifyChecksums(Map<String, List<String>> currentChecksums) {
        Map <String, List <Integer>> checksumViolationMap = new HashMap<>();
        currentChecksums.forEach((filePath, newChecksums) -> {
            List<String> oldChecksums = initialChecksums.get(filePath);
            if (oldChecksums == null) {
                System.out.println("New file detected: " + filePath);
                initialChecksums.put(filePath, new ArrayList<>(newChecksums));
                System.out.println("Checksums for new file added to initial checksums map.");
            } else {
                boolean mismatchFound = false;
                List <Integer> violationSlices = new ArrayList<>();
                for (int i = 0; i < newChecksums.size(); i++) {
                    if (!newChecksums.get(i).equals(oldChecksums.get(i))) {
                        System.out.printf("Checksum mismatch detected in %s at slice %d%n", filePath, i + 1);
                        mismatchFound = true;
                        violationSlices.add(i);
                        checksumViolationMap.put(filePath, violationSlices);
                        if (checksumViolationMap.containsKey(filePath)) {
                            checksumViolationMap.replace(filePath, violationSlices);
                        }
                    }
                }
                if (!mismatchFound) {
                    System.out.printf("No checksum mismatch detected for file %s%n", filePath);
                    checksumViolationMap.put(filePath, violationSlices);
                }
            }
        });
        return checksumViolationMap;
    }


    private void checkAndUpdateMetadata() {
        try {
            Files.walkFileTree(Path.of(this.fileStorageDirectory),
                    new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String key = file.toString();
                    ChunkMetaData metadata = chunkMetaDataMap.get(key);
                    if (metadata == null) {
                        // Create and add new metadata if it doesn't exist
                        metadata = new ChunkMetaData(1, 0, LocalDateTime.now(ZoneId.systemDefault())); // Initial version 1, sequence 0
                        metadata.setLastUpdated(attrs.creationTime().toInstant());
                        chunkMetaDataMap.put(key, metadata);
                        System.out.println("New metadata created for: " + file);
                    }
                    else if (metadata.getLastUpdatedMillis() < attrs.lastModifiedTime().toMillis()) {
                        metadata.incrementVersion();
                        metadata.setLastUpdated(FileTime.fromMillis(attrs.lastModifiedTime().toMillis()).toInstant());
                        chunkMetaDataMap.put(key, metadata); // Update the map with the new metadata
                        System.out.println("Metadata updated for: " + file);
                    }

                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setAndStartControllerConnection(TCPConnection controllerConnection) {
        this.controllerConnection = controllerConnection;
        this.controllerConnection.startConnection();
    }

    public String getDescriptor() {
        return descriptor;
    }


    /***
     * Message acknowledgments when ChunkServer is the receiver
     */
    public synchronized void receiveChunks (ChunkPayload chunkPayload) {
        System.out.println("Received chunk " + chunkPayload.getChunkWrapper().getChunkName());

        /***
         * First persist the chunk here locally and
         * then check the replicationPath and propagate the chunk to the other replicas
         * until we reach the terminal node.
         */
        FileUtils.storeFile(chunkPayload, this.fileStorageDirectory);

        /***
         * Verify that this chunk Server isn't the last element on the replication path
         * If it's not then it will pass the chunk over to another chunk server
         * on the replication path
         */
        String lastElInReplicationPath = chunkPayload.
                getReplicationPath().
                get(chunkPayload.getReplicationPath().size() - 1);
        int currentIndex = chunkPayload.getReplicationPath().indexOf(this.descriptor);
        if (!Objects.equals(this.getDescriptor(), lastElInReplicationPath)) {
            //forward chunks to next chunk server in the replication path
            String nextChunkServer = chunkPayload.getReplicationPath().get(currentIndex + 1);
            String nextChunkServerIp = nextChunkServer.split(":")[0];
            int nextChunkServerPort = Integer.parseInt(nextChunkServer.split(":")[1]);

            TCPConnection conn = getTCPConnection(tcpCache, nextChunkServerIp, nextChunkServerPort);
            try {
                conn.getSenderThread().sendData(chunkPayload);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        /***
         * Calculate the checksum for newly uploaded file
         */
        verifyCheckSumsAndInitiateRepair();

        if (FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())) {

            /***
             * If Reed solomon we also generate the shards
             */
            List<ShardWrapper> generatedShards = new ArrayList<>();
            if (FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())) {
                generatedShards = ReedSolomonFileEncoder.encodeShards(chunkPayload.getChunkWrapper());
            }
            /***
             * Now contact controller for a list of shard storage servers: todo
             */
            if (!chunksWithShards.contains(chunkPayload.getChunkWrapper().getChunkName())) {
                List<String> generatedShardNames = generatedShards.stream().map(ShardWrapper::getShardName)
                        .collect(Collectors.toList());

                for (ShardWrapper shard : generatedShards) {
                    shardWrapperMap.put(shard.getShardName(), shard);
                }

                /***
                 * After generating the shards, let us contact the controller node to get
                 * the shard storage nodes locations
                 */
                Message shardStorageLocationReqMsg = new Message(Protocol.SHARD_STORAGE_RANKING_REQUEST, this.descriptor
                        , generatedShardNames);
                System.out.println("Requested rankings for "+ generatedShardNames);

                try {
                    this.controllerConnection.getSenderThread().sendData(shardStorageLocationReqMsg);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                chunksWithShards.add(chunkPayload.getChunkWrapper().getChunkName());
            }
        }
    }
    /***
     * Received chunkwrapper from possibly a fellow chunk server holding a replica
     * during recovery via replication
     * so no need to deal with replication path
     * @param chunkWrapper
     */
    public synchronized void receiveChunksAsWrapper (ChunkWrapper chunkWrapper) {
        System.out.println("Received chunk "+ chunkWrapper.getChunkName());

        /***
         * First persist the chunk here locally and
         * then check the replicationPath and propagate the chunk to the other replicas
         * until we reach the terminal node.
         */
        //create dummy payload
        ChunkPayload chunkPayload = new ChunkPayload(chunkWrapper, new ArrayList<>());
        FileUtils.storeFile(chunkPayload, this.fileStorageDirectory);

        /***
         * chunkwrappers won't have replication paths because this is received from
         * a fellow chunk server during recovery procedures
         */

        /***
         * Calculate the checksum for newly uploaded file
         */
        verifyCheckSumsAndInitiateRepair();
    }

    /***
     * When client or another chunk server requests a chunk, send it over
     * @param conn
     * @param msg
     */
    public synchronized void sendChunks (TCPConnection conn, Message msg) {
        //first verify check sums are correct
        checksumsVerified = new CountDownLatch(1);
        verifyCheckSumsAndInitiateRepair();
        //wait for checkSumsToBeVerified
        try {
            checksumsVerified.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        String chunkToBeSent = (String) msg.getPayload();
        Path filePath = Paths.get(this.fileStorageDirectory, chunkToBeSent);

        try {
            // Read all bytes from the file
            byte[] fileData = Files.readAllBytes(filePath);

            // Extract the chunk name from the file path (assumes filePath is correctly formed)
            String chunkName = filePath.toString().replace(this.fileStorageDirectory, "");

            // Create a new ChunkWrapper with the read data
            ChunkWrapper chunk = new ChunkWrapper(fileData, chunkName, filePath.toString());

            // Send the ChunkWrapper object to the client
            conn.getSenderThread().sendObject(chunk);
            System.out.println("Sent chunk: " + chunkName + " to the requester");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error reading the file or sending data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /***
     * Controller returned the list of potentially pristine replicas. let's store that info
     * to state
     */
    public void receivePristineReplicaLocation (TCPConnection conn, Message msg) {
        this.nodesWithPureReplica = msg.getAdditionalPayload();
        waitForNodeInfoAboutPureReplicas.countDown();
        System.out.println("Nodes with pure replica as reported by Controller "+ this.nodesWithPureReplica);
    }

    /**
     * Retrieves specific slices from a chunk file as requested and hydrates a ChunkRepairResponse object.
     *
     * @param request the chunk repair request detailing which slices are corrupt.
     * @return ChunkRepairResponse containing the slices data for repair.
     */
    public void sendRequestedSlicesForRepair(TCPConnection conn, ChunkRepairRequest request) throws IOException {
        File file = new File(this.fileStorageDirectory + request.getChunkFullPath());
        Map<Integer, byte[]> chunkRepairMap = new ConcurrentHashMap<>();

        try (FileInputStream fis = new FileInputStream(file)) {
            for (Integer sliceIndex : request.getCorruptSlices()) {
                fis.getChannel().position((long) sliceIndex * ChunkServerConfig.MAX_SLICE_SIZE);
                byte[] sliceData = new byte[ChunkServerConfig.MAX_SLICE_SIZE];
                int bytesRead = fis.read(sliceData);
                if (bytesRead != ChunkServerConfig.MAX_SLICE_SIZE) {
                    // If less data is read than expected, copy the valid bytes
                    byte[] validData = new byte[bytesRead];
                    System.arraycopy(sliceData, 0, validData, 0, bytesRead);
                    sliceData = validData;
                }
                chunkRepairMap.put(sliceIndex, sliceData);
            }
        }

        ChunkRepairResponse response = new ChunkRepairResponse(chunkRepairMap);
        response.setChunkFullPath(request.getChunkFullPath());
        response.setChunkName(request.getChunkName());
        response.setCorruptSlices(new ArrayList<>(request.getCorruptSlices()));

        try {
            conn.getSenderThread().sendData(response);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * Time to repair the slices now
     * @param repairResponse
     */
    public void repairSlices(TCPConnection connection, ChunkRepairResponse repairResponse) {
        String chunkPath = repairResponse.getChunkFullPath(); // Get the full path to the chunk
        Map<Integer, byte[]> repairMap = repairResponse.getChunkRepairMap(); // Get the map of indices to slice data

        try (RandomAccessFile raf = new RandomAccessFile(this.fileStorageDirectory + chunkPath, "rw");
             FileChannel channel = raf.getChannel()) {

            for (Map.Entry<Integer, byte[]> entry : repairMap.entrySet()) {
                long position = (long) entry.getKey() * ChunkServerConfig.MAX_SLICE_SIZE; // Calculate the position in the file
                byte[] sliceData = entry.getValue(); // Get the replacement data for the slice
                channel.position(position); // Position the file channel
                channel.write(java.nio.ByteBuffer.wrap(sliceData)); // Write the replacement data
            }
            System.out.println("Repair completed for " + chunkPath);
            if (checksumsVerified != null) {
                checksumsVerified.countDown();
            }
            // Optionally log the successful repair or notify via TCPConnection
            connection.getSenderThread().sendObject("Repair completed for " + chunkPath);
        } catch (IOException e) {
            System.err.println("Error repairing file slices: " + e.getMessage());
            try {
                connection.getSenderThread().sendObject("Error during repair: " + e.getMessage());
            } catch (IOException ex) {
                System.err.println("Failed to send error message over TCP connection: " + ex.getMessage());
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /***
     * Elected as new backup, now got to fetch the lost replica to our storage
     * Use the recoveryMsg -> additionalPayload field since it contains
     * the info about live node which contains this replica
     * we'll request chunk from this node
     * @param recoveryMsg
     */
    public void recoverReplica(Message recoveryMsg) {
        System.out.println("Received replica recovery message");
        String lostChunkToRecover = (String) recoveryMsg.getPayload();
        String liveNodeWithReplica = recoveryMsg.getAdditionalPayload().get(0);

        String liveNodeWithReplicaIP = liveNodeWithReplica.split(":")[0];
        int liveNodeWithReplicaPort = Integer.parseInt(liveNodeWithReplica.split(":")[1]);
        TCPConnection connectionToLiveNode = getTCPConnection(tcpCache,
                liveNodeWithReplicaIP,
                liveNodeWithReplicaPort);

        Message chunkRequest = new Message(Protocol.REQUEST_CHUNK, lostChunkToRecover);
        try {
            connectionToLiveNode.getSenderThread().sendData(chunkRequest);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    /***
     * Receive shard storage server locations from controller
     */
    public void receiveShardStorageLocations (TCPConnection conn, Message message) {
        System.out.println("Received Shard storage locations ");
        List <List<String>> shardStorageServers = (List<List<String>>) message.getPayload();
        List <String> shardNames = message.getAdditionalPayload();

        int idx = 0;
        /***
         * Now send to target shard storage servers for persistence
         */
        for (List <String> server: shardStorageServers) {
            String targetServer = server.get(0); //target server for a shard (only 1)

            TCPConnection connPeer = getTCPConnection(tcpCache, targetServer.split(":")[0],
                    Integer.parseInt(targetServer.split(":")[1]));
            try {
                connPeer.getSenderThread().sendData(shardWrapperMap.get(shardNames.get(idx)));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            idx++;
        }
    }

    /***
     * Persist incoming shards
     */
    public void persistShard(TCPConnection conn, ShardWrapper shard) {
        System.out.println("Received shard " + shard.getShardName());
        shardWrapperMap.put(shard.getShardName(), shard);

        /***
         * First persist the chunk here locally and
         * then check the replicationPath and propagate the chunk to the other replicas
         * until we reach the terminal node.
         */
        FileUtils.storeShard(shard, this.fileStorageDirectory + "/" + ChunkServerConfig.SHARD_STORAGE_BASE);
        if (FAULT_TOLERANCE_MODE.equals(FaultToleranceMode.RS.getMode())) {
            if (shardsReceived != null) {
                shardsReceived.countDown();
            }
        }
    }


    /***
     * Controller initiated recovering via erasure coding through shard recover.
     * This targetServer looks at the shardMap and sends requests for the shards for the lost chunks
     *
     * @param conn
     * @param msg
     */
    public void recoverShards (TCPConnection conn, Message msg) {
        System.out.println("Recovering shards in backup server");

        Map <String, List<String>> shardLocations = (Map<String, List<String>>) msg.getPayload();


        shardsReceived = new CountDownLatch(shardLocations.size());
        for (Map.Entry<String, List <String>> shardInfo: shardLocations.entrySet()) {
            String targetNode = shardInfo.getValue().get(0);
            String shardName = shardInfo.getKey();

            if (targetNode.equals(this.descriptor)) {
                System.out.println("Self has shard. No need for TCP request. Continue to next shard");
                shardsReceived.countDown();
                continue;
            }
            TCPConnection connToTarget = getTCPConnection(tcpCache, targetNode.split(":")[0],
                    Integer.parseInt(targetNode.split(":")[1]));
            Message shardReq = new Message(Protocol.REQUEST_SHARD, shardName);

            try {
                connToTarget.getSenderThread().sendData(shardReq);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //wait for receipt of all shards
        try {
            shardsReceived.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //now reconstruct the chunks from shard
        System.out.println("All shards received. Now reconstructing chunk from shards");
        Set<String> chunkNames = new HashSet<>();

        for (String shardPath : shardLocations.keySet()) {
            // Assuming the chunk name is always followed by "_shard"
            int index = shardPath.indexOf("_shard");
            if (index != -1) {
                String chunkName = shardPath.substring(0, index);
                chunkNames.add(chunkName);
            }
        }
        ReedSolomonFileDecoder.reconstructFileFromShards(new ArrayList<>(chunkNames), this.fileStorageDirectory
                + "/" + ChunkServerConfig.SHARD_STORAGE_BASE, this.fileStorageDirectory);
        System.out.println("Reconstruction of lost chunk done");
    }

    /***
     * Send shard to server requesting shard. Typically part of recovery via erasure coding
     * @param conn
     * @param msg
     */
    public void sendShards (TCPConnection conn, Message msg) {
        String shardName = (String) msg.getPayload();
        ShardWrapper shard = shardWrapperMap.get(shardName);

        try {
            conn.getSenderThread().sendData(shard);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /***
     * TCP Cache
     */
    public TCPConnection getTCPConnection(Map<String, TCPConnection> tcpCache, String chunkServerIp, int chunkServerPort) {
        TCPConnection conn = null;
        if (tcpCache.containsKey(chunkServerIp+ ":" + chunkServerPort)) {
            conn = tcpCache.get(chunkServerIp+ ":" + chunkServerPort);
        }
        else {
            try {
                Socket clientSocket = new Socket(chunkServerIp, chunkServerPort);
                conn = new TCPConnection(this, clientSocket);
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
