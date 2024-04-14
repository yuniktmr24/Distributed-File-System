package csx55.dfs.replication;

import csx55.dfs.config.ChunkServerConfig;
import csx55.dfs.domain.ChunkMetaData;
import csx55.dfs.domain.Node;
import csx55.dfs.payload.ChunkPayload;
import csx55.dfs.payload.Message;
import csx55.dfs.transport.TCPConnection;
import csx55.dfs.transport.TCPServerThread;
import csx55.dfs.utils.ChunkWrapper;
import csx55.dfs.utils.FileChecksumCalculator;
import csx55.dfs.utils.FileUtils;
import csx55.dfs.payload.MajorHeartBeat;
import csx55.dfs.payload.MinorHeartBeat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ChunkServer implements Node {
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

    public static void main(String[] args) {
        //try (Socket socketToController = new Socket(args[0], Integer.parseInt(args[1]));
         try (Socket socketToController = new Socket("localhost", 12345);
             ServerSocket chunkServerSocket = new ServerSocket(0);
        ) {
            ChunkServer chunkServer = new ChunkServer();

             if (args.length == 1 && ChunkServerConfig.DEBUG_MODE) {
                 chunkServer.salt = Integer.parseInt(args[0]);
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
        try {
            this.controllerConnection.getSenderThread().sendData(minorHB);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void sendMajorHeartBeat() {
        lastMajorHeartbeat.set(System.currentTimeMillis());
        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Sending Major Heartbeat at: {0}", formatter.format(now));
        MajorHeartBeat majorHB = new MajorHeartBeat(getDescriptor(),
                ChunkServerConfig.DEBUG_MODE ? FileUtils.getNumberOfChunks(nodeIp, salt) : FileUtils.getNumberOfChunks(),
                ChunkServerConfig.DEBUG_MODE ? FileUtils.getAvailableStorage(nodeIp, salt) : FileUtils.getAvailableStorage());
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
        Runnable checksumVerificationTask = () -> {
            Map<String, List<String>> currentCheckSums = FileChecksumCalculator.getChecksumMapForChunkInDirectory(this.fileStorageDirectory);
            System.out.println("Scheduled checksum calculation completed. Total files processed: " + currentCheckSums.size());
            Map <String, List <Integer>> checksumViolationMap = verifyChecksums(currentCheckSums);

            for (Map.Entry<String, List<Integer>> chunkChecksum: checksumViolationMap.entrySet()) {
                List <Integer> checksumViolationSlices = chunkChecksum.getValue();
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

                }
            }
        };

        // Schedule the task to run every 15 seconds
        scheduler.scheduleAtFixedRate(checksumVerificationTask, 5, 15, TimeUnit.SECONDS);
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
    public void receiveChunks (ChunkPayload chunkPayload) {
        System.out.println("Received chunk "+ chunkPayload.getChunkWrapper().getChunkName());

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
    }

    /***
     * When client or another chunk server requests a chunk, send it over
     * @param conn
     * @param msg
     */
    public void sendChunks (TCPConnection conn, Message msg) {
        String chunkToBeSent = (String) msg.getPayload();
        Path filePath = Paths.get(this.fileStorageDirectory, chunkToBeSent).toAbsolutePath();

        try {
            // Read all bytes from the file
            byte[] fileData = Files.readAllBytes(filePath);

            // Extract the chunk name from the file path (assumes filePath is correctly formed)
            String chunkName = filePath.getFileName().toString();

            // Create a new ChunkWrapper with the read data
            ChunkWrapper chunk = new ChunkWrapper(fileData, chunkName, filePath.toString());

            // Send the ChunkWrapper object to the client
            conn.getSenderThread().sendObject(chunk);
            System.out.println("Sent chunk: " + chunkName + " to client.");
        } catch (IOException | InterruptedException e) {
            System.err.println("Error reading the file or sending data: " + e.getMessage());
            e.printStackTrace();
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
