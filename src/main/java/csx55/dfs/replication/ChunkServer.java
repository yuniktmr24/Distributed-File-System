package dfs.replication;

import dfs.config.ChunkServerConfig;
import dfs.domain.ChunkServerInfo;
import dfs.domain.Node;
import dfs.domain.UserCommands;
import dfs.payload.MajorHeartBeat;
import dfs.payload.MinorHeartBeat;
import dfs.transport.TCPConnection;
import dfs.transport.TCPServerThread;
import dfs.utils.FileUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
    public static void main(String[] args) {
        //try (Socket socketToController = new Socket(args[0], Integer.parseInt(args[1]));
         try (Socket socketToController = new Socket("localhost", 12345);
             ServerSocket chunkServerSocket = new ServerSocket(0);
        ) {
            ChunkServer chunkServer = new ChunkServer();
            chunkServer.setServiceDiscovery(InetAddress.getLocalHost().getHostAddress(), chunkServerSocket.getLocalPort());

            if (args.length == 1 && ChunkServerConfig.DEBUG_MODE) {
                chunkServer.salt = Integer.parseInt(args[0]);
            }

             /***
              * Set up controller connection
              */
            chunkServer.setAndStartControllerConnection(new TCPConnection(chunkServer, socketToController));

            Thread chunkServerThread = new Thread(new TCPServerThread(chunkServer, chunkServerSocket));
            chunkServerThread.start();

             /***
              * Setup the heartbeat transmission schedule
              */
            chunkServer.initiateHeartBeat();

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




    public void setAndStartControllerConnection(TCPConnection controllerConnection) {
        this.controllerConnection = controllerConnection;
        this.controllerConnection.startConnection();
    }

    public String getDescriptor() {
        return descriptor;
    }
}
