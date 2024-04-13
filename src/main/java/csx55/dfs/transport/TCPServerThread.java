package dfs.transport;


import dfs.domain.Node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServerThread implements Runnable {
    private ServerSocket serverSocket;

    private Node node;
    private TCPSenderThread senderThread;
    private TCPReceiverThread receiverThread;

    public TCPServerThread(Node node, ServerSocket serverSocket) {
        this.node = node;
        this.serverSocket = serverSocket;
    }

    public TCPSenderThread getSenderThread() {
        return senderThread;
    }

    public void setSenderThread(TCPSenderThread senderThread) {
        this.senderThread = senderThread;
    }

    public TCPReceiverThread getReceiverThread() {
        return receiverThread;
    }

    public void setReceiverThread(TCPReceiverThread receiverThread) {
        this.receiverThread = receiverThread;
    }

    @Override
    public void run() {
        System.out.println("Started Server thread at "+ serverSocket.getLocalPort());
        while (serverSocket != null) {
            try {
                //System.out.println("Server has new messages");
                Socket socket = serverSocket.accept();
                new TCPConnection(node, socket).startConnection();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
