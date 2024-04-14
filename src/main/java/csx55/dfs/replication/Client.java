package csx55.dfs.replication;

import csx55.dfs.domain.Node;
import csx55.dfs.domain.Protocol;
import csx55.dfs.domain.UserCommands;
import csx55.dfs.payload.ChunkPayload;
import csx55.dfs.payload.Message;
import csx55.dfs.transport.TCPConnection;
import csx55.dfs.utils.ChunkWrapper;
import csx55.dfs.utils.FileChunker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Client implements Node {
    private TCPConnection controllerConnection;

    private List <ChunkWrapper> chunks = new ArrayList<>();

    private Map <String, TCPConnection> tcpCache = new HashMap<>();
    public static void main (String [] args) {
        //try (Socket socketToController = new Socket(args[0], Integer.parseInt(args[1]));
         try (Socket socketToController = new Socket("localhost", 12345);
             ServerSocket clientSocket = new ServerSocket(0);)
        {
             Client client = new Client();

             client.setAndStartControllerConnection(new TCPConnection(client, socketToController));

             Thread userThread = new Thread(() -> client.userInput(client));
             userThread.start();

             while (true) {

             }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    /***
     * USER Inputs
     */
    private void userInput (Client node) {
        try {
            boolean running = true;
            while (running) {
                // Scanner scan = new Scanner(System.in);
                System.out.println("***************************************");
                System.out.println("[Messaging Node] Enter your Message Node command");
                System.out.println(UserCommands.clientCommandsToString());
                //String userInput = scan.nextLine();
                BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
                String userInput = inputReader.readLine();
                System.out.println("User input detected " + userInput);
                boolean containsSpace = false,
                        validUploadFilesCmd = false,
                        validDownloadFilesCmd = false;
                String uploadFilePath = ""; //local path to file to be uploaded
                String downloadFileName = "";
                if (userInput.contains(" ")) {
                    containsSpace = true;
                    if (userInput.startsWith(UserCommands.UPLOAD_FILE.getCmd()) ||
                            userInput.toUpperCase().contains("upload") ||
                            userInput.startsWith(String.valueOf(UserCommands.UPLOAD_FILE.getCmdId()))) {
                        validUploadFilesCmd = true;
                        uploadFilePath = userInput.split(" ")[1];
                    } else if (userInput.startsWith(UserCommands.DOWNLOAD_FILE.getCmd()) ||
                            userInput.toUpperCase().contains("download") ||
                            userInput.startsWith(String.valueOf(UserCommands.DOWNLOAD_FILE.getCmdId()))) {
                        validDownloadFilesCmd = true;
                        downloadFileName = userInput.split(" ")[1];
                    }
                }
                if (containsSpace && validUploadFilesCmd) {
                    try {
                        chunks = FileChunker.chunkFile(uploadFilePath);
                        System.out.println("Total chunks created: " + chunks.size());
                        for (ChunkWrapper chunk : chunks) {
                            System.out.println("Chunk " + chunk.getChunkName() + " size: " + chunk.getData().length + " bytes");
                        }
                        /***
                         * Cool. chunks have been created. Now let us contact the controller to
                         * get the top 3 available nodes for each chunk. (Rep. factor = 3)
                         */
                        List <String> chunkNames = chunks.stream().map(ChunkWrapper::getChunkName).collect(Collectors.toList());

                        Message rankingMsg = new Message(Protocol.CHUNK_SERVER_RANKING_REQUEST, chunks.size(), chunkNames);
                        controllerConnection.getSenderThread().sendData(rankingMsg);

                    } catch (IOException e) {
                        System.err.println("Error processing file: " + e.getMessage());
                    }
                }
            }

        }
        catch (Exception ex) {

        }
    }

    public void setAndStartControllerConnection(TCPConnection controllerConnection) {
        this.controllerConnection = controllerConnection;
        this.controllerConnection.startConnection();
    }

    /***
     * Message acknowledgments when Client is the receiver
     */
    public void receiveChunkServerRankingFromController(List<List<String>> rankedServers) {
        for (int i = 0; i < rankedServers.size(); i++) {
            System.out.println("Chunk " + (i + 1) + " servers: " + rankedServers.get(i));
            List <String> chunkServersInfo = rankedServers.get(i);

            /***
             * Now lets use the first ranked chunkServer as the entry point for client - chunkServer
             * data plane traffic
             */
            String entryChunkServerIP = chunkServersInfo.get(0).split(":")[0];
            int entryChunkServerPort = Integer.parseInt(chunkServersInfo.get(0).split(":")[1]);

            TCPConnection connectionToEntryChunkServer = getTCPConnection(tcpCache, entryChunkServerIP, entryChunkServerPort);

            //chunk wrapper for i-th chunk
            ChunkWrapper chunkWrapper = chunks.get(i);

            ChunkPayload chunkPayload = new ChunkPayload(chunkWrapper, chunkServersInfo);
            try {
                connectionToEntryChunkServer.getSenderThread().sendData(chunkPayload);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
