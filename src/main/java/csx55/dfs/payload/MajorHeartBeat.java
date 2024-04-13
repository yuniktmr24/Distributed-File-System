package csx55.dfs.payload;

import java.io.Serializable;

public class MajorHeartBeat extends HeartBeat implements Serializable {
    private static final long serialVersionUID = 1L;
    /***
     * @serialField info about the metadata for all files stored in a given chunkserver
     */

    public MajorHeartBeat(String origin, int totalNumberOfChunks, long freeSpaceAvailable) {
        super(origin, totalNumberOfChunks, freeSpaceAvailable);
    }



}
