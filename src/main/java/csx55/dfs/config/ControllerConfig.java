package csx55.dfs.config;

import java.util.concurrent.TimeUnit;

public class ControllerConfig {
    public static final long HEARTBEAT_TIMEOUT = TimeUnit.MINUTES.toMillis(2);  // 2 minutes timeout
}
