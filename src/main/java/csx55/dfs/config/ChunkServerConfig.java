package csx55.dfs.config;

import java.util.concurrent.TimeUnit;

public class ChunkServerConfig {
    public static final String CHUNK_STORAGE_ROOT_DIRECTORY = "/tmp/ytamraka/demo1";

    public static final int MAX_SLICE_SIZE = 8 * 1024; // 8KB slices

    public static final long CHUNK_SERVER_INITIAL_FREE_SPACE = 1000000; //in KBs

    public static final String CHUNK_STORAGE_EXT = "chunk";

    public static final String SHARD_EXT = "shard";

    public static final String SHARD_STORAGE_BASE = "shards";

    public static final boolean DEBUG_MODE = false;
}
