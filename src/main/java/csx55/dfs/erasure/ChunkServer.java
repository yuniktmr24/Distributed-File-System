package csx55.dfs.erasure;

import csx55.dfs.config.ChunkServerConfig;

public class ChunkServer {
    public static void main (String [] args) {
        csx55.dfs.replication.ChunkServer.main(
                ChunkServerConfig.DEBUG_MODE ?
                        new String[]{args[0], args[1], "Reed-Solomon", args[2]}
                        : new String[]{args[0], args[1], "Reed-Solomon"});
    }
}
