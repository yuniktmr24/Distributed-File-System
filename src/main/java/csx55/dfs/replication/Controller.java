package dfs.replication;

import dfs.config.ControllerConfig;
import dfs.domain.ChunkServerInfo;
import dfs.domain.Node;
import dfs.payload.MajorHeartBeat;
import dfs.payload.MinorHeartBeat;
import dfs.transport.TCPServerThread;

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
    private static final Logger logger = Logger.getLogger(Controller.class.getName());
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static List<ChunkServerInfo> chunkServerInfo = Collections.synchronizedList(new ArrayList<>());
    private static Map<String, Long> lastHeartbeatReceived = new ConcurrentHashMap<>();

    private static Map <String, ChunkServerInfo> chunkServerInfoMap = new ConcurrentHashMap<>();

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
                }
            });
        }, 1, 1, TimeUnit.MINUTES); // Check every minute
    }

}
