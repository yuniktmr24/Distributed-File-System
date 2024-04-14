package csx55.dfs.payload;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Message implements Serializable {

    private static final long serialversionUID = 1L;
    private int protocol;

    private Object payload;

    private List<String> additionalPayload = new ArrayList<>();

    public Message(int protocol, Object payload) {
        this.protocol = protocol;
        this.payload = payload;
    }

    public Message(int protocol, Object payload, List<String> additionalPayload) {
        this.protocol = protocol;
        this.payload = payload;
        this.additionalPayload = additionalPayload;
    }

    public int getProtocol() {
        return protocol;
    }

    public Object getPayload() {
        return payload;
    }

    public List<String> getAdditionalPayload() {
        return additionalPayload;
    }
}
