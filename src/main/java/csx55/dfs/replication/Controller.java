package csx55.dfs.replication;

import csx55.dfs.config.ControllerConfig;
import csx55.dfs.domain.ChunkMetaData;
import csx55.dfs.domain.ChunkServerInfo;
import csx55.dfs.domain.Node;
import csx55.dfs.payload.MajorHeartBeat;
import csx55.dfs.payload.MinorHeartBeat;
import csx55.dfs.transport.TCPServerThread;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Controller implements Node {
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
    private static Map <String, ChunkServerInfo> chunkServerInfoMap;

    private static Map <String, List <ChunkMetaData>> chunkMetaDataMap;

    private static Map <String, List <String>> chunkStorageMap;

    private static Map <String, Long> chunkServerAvailableSpaceMap;

    private static Map<String, Long> lastHeartbeatReceived;

    static {
        chunkServerInfoMap = new ConcurrentHashMap<>();
        chunkMetaDataMap = new ConcurrentHashMap<>();
        chunkStorageMap = new ConcurrentHashMap<>();
        chunkServerAvailableSpaceMap = new ConcurrentHashMap<>();
        lastHeartbeatReceived = new ConcurrentHashMap<>();
    }
    private static final Logger logger = Logger.getLogger(Controller.class.getName());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Controller instance = new Controller();
    public static Controller getInstance() {
        return instance;
    }
    private static ScheduledExecutorService heartbeatChecker = Executors.newSingleThreadScheduledExecutor();
    public static void main (String [] args) {
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
        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Received major heart beat at: {0}", formatter.format(now));
        lastHeartbeatReceived.put(majorHb.getHeartBeatOrigin(), System.currentTimeMillis());
        System.out.println(majorHb.toString());
    }

    public synchronized void receiveMinorHeartBeat(MinorHeartBeat minorHb) {
        LocalDateTime now = LocalDateTime.now();
        logger.log(Level.INFO, "Received minor heart beat at: {0}", formatter.format(now));
        lastHeartbeatReceived.put(minorHb.getHeartBeatOrigin(), System.currentTimeMillis());
        System.out.println(minorHb.toString());
    }

    private static void setupHeartbeatChecker() {
        heartbeatChecker.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            lastHeartbeatReceived.forEach((key, lastTime) -> {
                if ((now - lastTime) > ControllerConfig.HEARTBEAT_TIMEOUT) {
                    logger.warning("Heartbeat timeout for server: " + key);
                    // Here you might also want to try reconnecting or marking the server as down.
                    //TODO fault tolerance. File transfers via data plane
                }
            });
        }, 1, 1, TimeUnit.MINUTES); // Check every minute
    }

}
