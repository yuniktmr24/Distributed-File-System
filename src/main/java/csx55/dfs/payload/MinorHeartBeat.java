package csx55.dfs.payload;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MinorHeartBeat extends HeartBeat implements Serializable {
    private static final long serialVersionUID = 1L;

    /***
     * New chunk files since last minor heartbeat
     */
    private List<String> newChunkFiles = new ArrayList<>();

    private List<String> newShards = new ArrayList<>();
    public MinorHeartBeat(String origin, int totalNumberOfChunks, long freeSpaceAvailable) {
        super(origin, totalNumberOfChunks, freeSpaceAvailable);
    }

    public List<String> getNewChunkFiles() {
        return newChunkFiles;
    }

    public void setNewChunkFiles(List<String> newChunkFiles) {
        this.newChunkFiles = newChunkFiles;
    }

    public List<String> getNewShards() {
        return newShards;
    }

    public void setNewShards(List<String> newShards) {
        this.newShards = newShards;
    }
}
