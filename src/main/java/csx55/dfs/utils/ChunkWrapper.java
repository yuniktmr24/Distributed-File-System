package csx55.dfs.utils;

public class ChunkWrapper {
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
