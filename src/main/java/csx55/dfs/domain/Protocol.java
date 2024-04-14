package csx55.dfs.domain;

public interface Protocol {
    final int CHUNK_SERVER_RANKING_REQUEST = 1;

    final int CHUNK_SERVER_RANKING_RESPONSE = 2;

    final int REPLICA_LOCATION_REQUEST = 3;

    final int REPLICA_LOCATION_RESPONSE = 4;

    final int REQUEST_CHUNK = 5;

    final int PRISTINE_CHUNK_LOCATION_REQUEST = 6;

    final int PRISTINE_CHUNK_LOCATION_RESPONSE = 7;
}

