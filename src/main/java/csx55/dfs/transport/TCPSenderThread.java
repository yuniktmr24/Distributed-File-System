package dfs.transport;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class TCPSenderThread implements Runnable {

    protected ObjectOutputStream dout;

    private LinkedBlockingQueue<Object> queue;


    public TCPSenderThread(Socket socket) throws IOException {
        final int defaultQueueSize = 1000;
        this.queue = new LinkedBlockingQueue<>( defaultQueueSize );
        this.dout = new ObjectOutputStream( socket.getOutputStream() );
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
                    System.out.println("Connection closed");
                    //e.printStackTrace();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
