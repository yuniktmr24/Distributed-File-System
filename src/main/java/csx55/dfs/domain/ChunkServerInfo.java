package csx55.dfs.domain;

import java.util.ArrayList;
import java.util.List;

public class ChunkServerInfo {
    private String nodeIp;

    private int nodePort;

    private long availableSpace;


    //metadata info for all chunk files in the chunkServer
    private List<ChunkMetaData> metaData = new ArrayList<>();

    public ChunkServerInfo(String nodeIp, int nodePort) {
        this.nodeIp = nodeIp;
        this.nodePort = nodePort;
    }

    public String getNodeIp() {
        return nodeIp;
    }

    public void setNodeIp(String nodeIp) {
        this.nodeIp = nodeIp;
    }

    public int getNodePort() {
        return nodePort;
    }

    public void setNodePort(int nodePort) {
        this.nodePort = nodePort;
    }

    public long getAvailableSpace() {
        return availableSpace;
    }

    public void setAvailableSpace(long availableSpace) {
        this.availableSpace = availableSpace;
    }
}

