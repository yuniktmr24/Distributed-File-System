package csx55.dfs.transport;


import csx55.dfs.domain.Protocol;
import csx55.dfs.payload.*;
import csx55.dfs.replication.Controller;
import csx55.dfs.domain.Node;
import csx55.dfs.replication.ChunkServer;
import csx55.dfs.replication.Client;
import csx55.dfs.utils.ChunkWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.List;

public class TCPReceiverThread implements Runnable {
    private Socket messageSource;
    private Node node;
    private ObjectInputStream ois;

    private byte[] receivedPayload;

    private TCPConnection connection;

    private boolean terminated = false;

    public TCPReceiverThread (Node node, Socket socket, TCPConnection connection) throws IOException {
        try {
            messageSource = socket;
            this.node = node;
            ois = new ObjectInputStream(socket.getInputStream());
            this.connection = connection;
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void terminateReceiver(){
        terminated = true;
    }

    public byte[] getReceivedPayload() {
        return receivedPayload;
    }

    private void setReceivedPayload(byte[] receivedPayload) throws IOException {
        this.receivedPayload = receivedPayload;
    }

    @Override
    public void run() {
        while (true) {
            Serializable object;
            try {
                object = readObject(ois);
                /***
                 * Controller operations
                 */
                if (node instanceof Controller) {
                    Controller controller = ((Controller) node);
                    if (object instanceof MajorHeartBeat) {
                        controller.receiveMajorHeartBeat((MajorHeartBeat) object);
                    }
                    else if (object instanceof MinorHeartBeat) {
                        controller.receiveMinorHeartBeat((MinorHeartBeat) object);
                    }
                    else if (object instanceof Message) {
                        Message msg = (Message) object;
                        if (msg.getProtocol() == Protocol.CHUNK_SERVER_RANKING_REQUEST) {
                            controller.generateChunkServerRankingForClient(connection, (List <Long>) msg.getPayload(), msg.getAdditionalPayload());
                        }
                        else if (msg.getProtocol() == Protocol.REPLICA_LOCATION_REQUEST) {
                            controller.sendReplicaLocations(connection, msg);
                        }
                        else if (msg.getProtocol() == Protocol.PRISTINE_CHUNK_LOCATION_REQUEST) {
                            controller.sendPristineReplicaLocation(connection, msg);
                        }
                    }
                }
                /***
                 *  Chunk Server operations
                 */
                else if (node instanceof ChunkServer) {
                    ChunkServer chunkServer = (ChunkServer) node;
                    if (object instanceof ChunkPayload) {
                        /***
                         * well this means we have received chunks
                         */
                        chunkServer.receiveChunks((ChunkPayload) object);
                    }
                    else if (object instanceof ChunkWrapper) {
                        chunkServer.receiveChunksAsWrapper((ChunkWrapper) object);
                    }
                    else if (object instanceof Message) {
                        Message msg = (Message) object;
                        if (msg.getProtocol() == Protocol.REQUEST_CHUNK) {
                            chunkServer.sendChunks(connection, msg);
                        }
                        else if (msg.getProtocol() == Protocol.PRISTINE_CHUNK_LOCATION_RESPONSE) {
                            chunkServer.receivePristineReplicaLocation(connection, msg);
                        }
                        else if (msg.getProtocol() == Protocol.RECOVER_REPLICA) {
                            //when one node goes down, and this node gets elected as backup
                            //for replica
                            chunkServer.recoverReplica(msg);
                        }
                    }
                    /***
                     * The chunkserver with corrupted slice has requested us for the
                     * pristine slice
                     */
                    else if (object instanceof ChunkRepairRequest) {
                        ChunkRepairRequest repairRequest = (ChunkRepairRequest) object;
                        chunkServer.sendRequestedSlicesForRepair(connection, repairRequest);
                    }
                    /***
                     * The chunkServer with pristine copy of the replica has replied
                     */
                    else if (object instanceof ChunkRepairResponse) {
                        ChunkRepairResponse repairResponse = (ChunkRepairResponse) object;
                        chunkServer.repairSlices(connection, repairResponse);
                    }

                }

                /***
                 * Client operations
                 */
                else if (node instanceof Client) {
                    Client client = (Client) node;
                    if (object instanceof Message) {
                        Message msg = (Message) object;
                        if (msg.getProtocol() == Protocol.CHUNK_SERVER_RANKING_RESPONSE) {
                            client.receiveChunkServerRankingFromController((List<List<String>>) msg.getPayload());
                        }
                    }
                    else if (object instanceof ChunkLocationPayload) {
                        client.receiveChunkReplicationLocationsFromController((ChunkLocationPayload) object);
                    }
                    else if (object instanceof ChunkWrapper) {
                        client.receiveChunkFromChunkServer((ChunkWrapper) object);
                    }
                }
            } catch (Exception ex) {
                this.close();
            }
        }
    }

    private Serializable readObject (ObjectInputStream ois) {
        try {
            return (Serializable) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }


    public void close() {
        try {
            this.ois.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

