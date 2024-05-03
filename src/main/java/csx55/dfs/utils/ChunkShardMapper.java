package csx55.dfs.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChunkShardMapper {

    // Method to get shard locations for a specific chunk
    public static Map<String, List<String>> getShardLocations(String chunkName, Map<String, List<String>> allShards) {
        Map<String, List<String>> shardMap = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : allShards.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();

            if (key.contains(chunkName)) { // Check if the key contains the chunk name
                shardMap.put(key, new ArrayList<>(value)); // Store in new map
            }
        }

        return shardMap;
    }

    public static void main(String[] args) {
        // Prepare the map with shard details
        Map<String, List<String>> shardDetails = new HashMap<>();
        shardDetails.put("/files/BigChunk.txt_chunk8_shard_8", List.of("127.0.0.1:49445"));
        shardDetails.put("/files/BigChunk.txt_chunk8_shard_4", List.of("127.0.0.1:49445"));
        shardDetails.put("/files/BigChunk.txt_chunk8_shard_5", List.of("127.0.0.1:49445"));
        // Add all other shards similarly...

        // Get locations for a specific chunk
        Map<String, List<String>> shardLocations = getShardLocations("/files/BigChunk.txt_chunk8", shardDetails);

        // Print the results
        for (Map.Entry<String, List<String>> entry : shardLocations.entrySet()) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }
}
