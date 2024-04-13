package dfs.transport;


import dfs.domain.Node;
import dfs.payload.HeartBeat;
import dfs.payload.MajorHeartBeat;
import dfs.payload.MinorHeartBeat;
import dfs.replication.ChunkServer;
import dfs.replication.Client;
import dfs.replication.Controller;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.Socket;

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
                    if (object instanceof MajorHeartBeat) {
                        ((Controller) node).receiveMajorHeartBeat((MajorHeartBeat) object);
                    }
                    else if (object instanceof MinorHeartBeat) {
                        ((Controller)node).receiveMinorHeartBeat((MinorHeartBeat) object);
                    }
                }
                /***
                 *  Chunk Server operations
                 */
                else if (node instanceof ChunkServer) {

                }

                /***
                 * Client operations
                 */
                else if (node instanceof Client) {

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

