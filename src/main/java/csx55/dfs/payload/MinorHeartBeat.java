package dfs.payload;

import java.io.Serializable;

public class MinorHeartBeat extends HeartBeat implements Serializable {
    private static final long serialVersionUID = 1L;
    public MinorHeartBeat(String origin, int totalNumberOfChunks, long freeSpaceAvailable) {
        super(origin, totalNumberOfChunks, freeSpaceAvailable);
    }
}
