package csx55.dfs.utils;

public class ShardWrapper extends ChunkWrapper {
    private String shardName;
    public ShardWrapper(byte[] data, String chunkName, String filePath) {
        super(data, chunkName, filePath);
    }

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }
}
