package csx55.dfs.utils;

import java.util.*;
import java.util.stream.Collectors;

public class ChunkServerRanker {

    private static final Map<String, Long> chunkServerAvailableSpaceMap = new HashMap<>();

    public static void main(String[] args) {
        // Sample data for available space
        chunkServerAvailableSpaceMap.put("Server1", 1000L);
        chunkServerAvailableSpaceMap.put("Server2", 1500L);
        chunkServerAvailableSpaceMap.put("Server3", 1200L);
        chunkServerAvailableSpaceMap.put("Server4", 800L);
        chunkServerAvailableSpaceMap.put("Server5", 1300L);

        // Request to place 3 chunks
        List<List<String>> serversForChunks = rankChunkServersForChunks(3, chunkServerAvailableSpaceMap);
        for (int i = 0; i < serversForChunks.size(); i++) {
            System.out.println("Chunk " + (i + 1) + " servers: " + serversForChunks.get(i));
        }
    }

    /**
     * Ranks available chunk servers based on their free space and returns a list of server IDs
     * for the top three choices for each chunk.
     *
     * @param numChunks the number of chunks to place
     * @return a list of lists, where each inner list contains the top three server IDs for each chunk
     */
    public static List<List<String>> rankChunkServersForChunks(int numChunks, Map<String, Long> chunkServerAvailableSpaceMap) {
        List<List<String>> serverAssignmentsForChunks = new ArrayList<>();
        List<Map.Entry<String, Long>> sortedServers = new ArrayList<>(chunkServerAvailableSpaceMap.entrySet());

        // Sort servers by available space in descending order
        sortedServers.sort(Map.Entry.<String, Long>comparingByValue().reversed());

        // Collect top 3 servers based on available space
        List<String> topServers = sortedServers.stream()
                .limit(3)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Assign these top servers to each chunk
        for (int chunkIndex = 0; chunkIndex < numChunks; chunkIndex++) {
            serverAssignmentsForChunks.add(new ArrayList<>(topServers));
        }

        return serverAssignmentsForChunks;
    }
}
