package csx55.dfs.payload;

import csx55.dfs.utils.ChunkWrapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ChunkPayload implements Serializable {
    private static final long serialVersionUID = 1L;
    private ChunkWrapper chunkWrapper;


    /***
     * @param: replicationPath: the nodes in which the given chunk is to be replicated:
     * if replicationPath = "telluride@cs.colostate.edu:12345, falcon@cs.colostate.edu:12346, denver@cs.colostate.edu:34567"
     * then the data plane traffic first goes to telluride and from there
     * it goes to falcon and denver
     */
    private List<String> replicationPath = new ArrayList<>();

    public ChunkPayload(ChunkWrapper chunkWrapper, List <String> replicationPath) {
        this.chunkWrapper = chunkWrapper;
        this.replicationPath = replicationPath;
    }

    public ChunkWrapper getChunkWrapper() {
        return chunkWrapper;
    }

    public List <String> getReplicationPath() {
        return replicationPath;
    }
}
