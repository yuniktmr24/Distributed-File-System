package csx55.dfs.erasure;

public class ChunkServer {
    public static void main (String [] args) {
        csx55.dfs.replication.ChunkServer.main(new String[]{args[0], args[1], "Reed-Solomon"});
    }
}
