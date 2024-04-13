package dfs.config;

import java.util.concurrent.TimeUnit;

public class ChunkServerConfig {
    public static final String CHUNK_STORAGE_ROOT_DIRECTORY = "tmp";

    public static final long CHUNK_SERVER_INITIAL_FREE_SPACE = 1000000; //in KBs

    public static final String CHUNK_STORAGE_EXT = "chunk";

    public static final boolean DEBUG_MODE = true;
}
