package csx55.dfs.domain;

public enum FaultToleranceMode {
    REPLICATION("Replication"), RS("Reed-Solomon");

    private final String mode;

    private FaultToleranceMode(String mode) {
        this.mode = mode;
    }

    public String getMode() {
        return mode;
    }
}
