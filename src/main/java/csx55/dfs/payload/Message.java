package csx55.dfs.payload;

import java.io.Serializable;

public class Message implements Serializable {

    private static final long serialversionUID = 1L;
    private int protocol;

    private Object payload;

    public Message(int protocol, Object payload) {
        this.protocol = protocol;
        this.payload = payload;
    }

    public int getProtocol() {
        return protocol;
    }

    public Object getPayload() {
        return payload;
    }
}
