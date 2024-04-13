package dfs.payload;


import dfs.domain.ChunkMetaData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class HeartBeat implements Serializable {
    private static final long serialVersionUID = 1L;

    private String heartBeatOrigin;
    private int totalNumberOfChunks;

    private long freeSpaceAvailable;

    private List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();

    public HeartBeat(String heartBeatOrigin, int totalNumberOfChunks, long freeSpaceAvailable) {
        this.heartBeatOrigin = heartBeatOrigin;
        this.totalNumberOfChunks = totalNumberOfChunks;
        this.freeSpaceAvailable = freeSpaceAvailable;
    }

    public int getTotalNumberOfChunks() {
        return totalNumberOfChunks;
    }

    public void setTotalNumberOfChunks(int totalNumberOfChunks) {
        this.totalNumberOfChunks = totalNumberOfChunks;
    }

    public long getFreeSpaceAvailable() {
        return freeSpaceAvailable;
    }

    public void setFreeSpaceAvailable(long freeSpaceAvailable) {
        this.freeSpaceAvailable = freeSpaceAvailable;
    }

    public String getHeartBeatOrigin() {
        return heartBeatOrigin;
    }

    @Override
    public String toString() {
        return "HeartBeat{" +
                "heartBeatOrigin='" + heartBeatOrigin + '\'' +
                ", totalNumberOfChunks=" + totalNumberOfChunks +
                ", freeSpaceAvailable=" + freeSpaceAvailable +
                '}';
    }
}
