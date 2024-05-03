package csx55.dfs.config;

import java.util.concurrent.TimeUnit;

public class ControllerConfig {
    public static final long HEARTBEAT_TIMEOUT = TimeUnit.SECONDS.toMillis(25);  // 2 minutes timeout

    public static final int NUM_CHUNKS = 3;

    public static final int K_SHARD_STORAGE_SERVERS = 1;
}
