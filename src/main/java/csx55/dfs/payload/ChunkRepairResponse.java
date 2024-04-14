package csx55.dfs.payload;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChunkRepairResponse implements Serializable {
    /***
     * chunkRepairMap keeps track of the chunk-index and the healthy slice retrieved
     * from the pristine replica
     */
    private Map<Integer, byte []> chunkRepairMap = new ConcurrentHashMap<>();

    private String chunkFullPath;

    private String chunkName;

    //original list of corrupt slices
    private List<Integer> corruptSlices = new ArrayList<>(); //0-based indices

    public ChunkRepairResponse(Map<Integer, byte[]> chunkRepairMap) {
        this.chunkRepairMap = chunkRepairMap;
    }

    public Map<Integer, byte[]> getChunkRepairMap() {
        return chunkRepairMap;
    }

    public void setChunkRepairMap(Map<Integer, byte[]> chunkRepairMap) {
        this.chunkRepairMap = chunkRepairMap;
    }

    public String getChunkFullPath() {
        return chunkFullPath;
    }

    public void setChunkFullPath(String chunkFullPath) {
        this.chunkFullPath = chunkFullPath;
    }

    public String getChunkName() {
        return chunkName;
    }

    public void setChunkName(String chunkName) {
        this.chunkName = chunkName;
    }

    public List<Integer> getCorruptSlices() {
        return corruptSlices;
    }

    public void setCorruptSlices(List<Integer> corruptSlices) {
        this.corruptSlices = corruptSlices;
    }
}
