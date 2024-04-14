package csx55.dfs.payload;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChunkLocationPayload implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, List<String>> chunkLocations = new HashMap<>();


    public ChunkLocationPayload(Map<String, List<String>> chunkLocations) {
        this.chunkLocations = chunkLocations;
    }

    public Map<String, List<String>> getChunkLocations() {
        return chunkLocations;
    }
}
