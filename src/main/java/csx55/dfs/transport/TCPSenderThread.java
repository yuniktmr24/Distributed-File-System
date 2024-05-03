package csx55.dfs.transport;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPSenderThread implements Runnable {

    protected ObjectOutputStream dout;

    private LinkedBlockingQueue<Object> queue;

    private Socket socket;


    public TCPSenderThread(Socket socket) throws IOException {
        final int defaultQueueSize = 1000;
        this.queue = new LinkedBlockingQueue<>( defaultQueueSize );
        this.dout = new ObjectOutputStream( socket.getOutputStream() );
        this.socket = socket;
    }

    public void sendData(final Object obj) throws InterruptedException {
        queue.put( obj );
    }

    public void sendObject(final Object obj) throws IOException, InterruptedException {
       queue.put(obj);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Object obj = queue.take();
                try {
                    dout.writeObject(obj);
                    dout.flush();
                    dout.reset();
                } catch (IOException e) {
                    System.out.println("Connection closed to "+ socket.getInetAddress() + ":" + socket.getPort());
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
