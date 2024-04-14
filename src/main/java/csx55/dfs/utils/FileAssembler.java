package csx55.dfs.utils;

import csx55.dfs.utils.ChunkWrapper;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Comparator;

public class FileAssembler {

    /**
     * Assembles the original file from a list of chunk wrappers and writes the output to the specified path.
     * @param chunks The list of chunk wrappers containing the data and chunk names.
     * @param outputFileFullPath The full path to the output file where the assembled file will be saved.
     */
    public static void assembleChunks(List<ChunkWrapper> chunks, String outputFileFullPath) {
        try {
            // Sort the chunks based on the numeric part of the chunkName
            chunks.sort(Comparator.comparingInt(chunk -> Integer.parseInt(chunk.getChunkName().replaceAll("[^0-9]", ""))));

            // Create output file stream
            try (FileOutputStream outputStream = new FileOutputStream(outputFileFullPath)) {
                for (ChunkWrapper chunk : chunks) {
                    // Write each chunk's data to the output file in the correct order
                    outputStream.write(chunk.getData());
                }
            }

            System.out.println("All chunks have been assembled into: " + outputFileFullPath);
        } catch (IOException e) {
            System.err.println("Failed to assemble chunks: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
