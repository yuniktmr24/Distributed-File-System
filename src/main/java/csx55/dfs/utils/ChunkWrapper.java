package csx55.dfs.utils;

public class ChunkWrapper {
    private byte[] data;
    private String chunkName;

    public ChunkWrapper(byte[] data, String chunkName) {
        this.data = data;
        this.chunkName = chunkName;
    }

    public byte[] getData() {
        return data;
    }

    public String getChunkName() {
        return chunkName;
    }
}
