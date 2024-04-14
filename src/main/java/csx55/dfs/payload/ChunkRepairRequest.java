package csx55.dfs.payload;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ChunkRepairRequest implements Serializable {
    /***
     * @param origin: Node descriptor where the chunk is tampered with
     */
    private String origin;

    private String chunkFullPath;

    private String chunkName;

    /***
     * Slices of the chunk that were detected as corrupt
     */
    private List<Integer> corruptSlices = new ArrayList<>(); //0-based indices

    public ChunkRepairRequest(String origin, String chunkFullPath, List<Integer> corruptSlices) {
        this.origin = origin;
        this.chunkFullPath = chunkFullPath;
        this.corruptSlices = corruptSlices;
    }

    public ChunkRepairRequest(String origin, String chunkFullPath, String chunkName, List<Integer> corruptSlices) {
        this.origin = origin;
        this.chunkFullPath = chunkFullPath;
        this.chunkName = chunkName;
        this.corruptSlices = corruptSlices;
    }

    public String getOrigin() {
        return origin;
    }

    public String getChunkFullPath() {
        return chunkFullPath;
    }

    public String getChunkName() {
        return chunkName;
    }

    public List<Integer> getCorruptSlices() {
        return corruptSlices;
    }

    @Override
    public String toString() {
        return "ChunkRepairRequest{" +
                "origin='" + origin + '\'' +
                ", chunkFullPath='" + chunkFullPath + '\'' +
                ", chunkName='" + chunkName + '\'' +
                ", corruptSlices=" + corruptSlices +
                '}';
    }
}
