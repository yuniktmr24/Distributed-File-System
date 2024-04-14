package csx55.dfs.utils;

import java.io.Serializable;

public class ChunkWrapper implements Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] data;
    private String chunkName;

    private String filePath;

    public ChunkWrapper(byte[] data, String chunkName, String filePath) {
        this.data = data;
        this.chunkName = chunkName;
        this.filePath = filePath;
    }

    public byte[] getData() {
        return data;
    }

    public String getChunkName() {
        return chunkName;
    }

    public String getFilePath() {
        return filePath;
    }
}
