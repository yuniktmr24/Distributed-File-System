package dfs.payload;

import dfs.domain.ChunkMetaData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MajorHeartBeat extends HeartBeat implements Serializable {
    private static final long serialVersionUID = 1L;
    /***
     * @serialField info about the metadata for all files stored in a given chunkserver
     */

    public MajorHeartBeat(String origin, int totalNumberOfChunks, long freeSpaceAvailable) {
        super(origin, totalNumberOfChunks, freeSpaceAvailable);
    }



}
