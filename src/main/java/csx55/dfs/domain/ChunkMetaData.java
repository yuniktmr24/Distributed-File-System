package csx55.dfs.domain;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class ChunkMetaData {
    private int version;
    private int sequenceNumber;
    private LocalDateTime lastUpdated;

    public ChunkMetaData(int version, int sequenceNumber, LocalDateTime lastUpdated) {
        this.version = version;
        this.sequenceNumber = sequenceNumber;
        this.lastUpdated = lastUpdated;
    }

    public void incrementVersion() {
        this.version++;
    }

    public LocalDateTime getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Instant lastUpdated) {
        this.lastUpdated = LocalDateTime.ofInstant(lastUpdated, ZoneId.systemDefault());
    }

    public long getLastUpdatedMillis() {
        // Convert LocalDateTime to Instant using the system default time zone and then to epoch milli
        return lastUpdated.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

}
