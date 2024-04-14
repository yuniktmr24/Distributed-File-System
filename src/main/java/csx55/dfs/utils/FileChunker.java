package csx55.dfs.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileChunker {

    private static final int CHUNK_SIZE = 64 * 1024; // 64 KB

    /**
     * Divides a file into 64KB chunks, wrapping each chunk with a ChunkWrapper.
     *
     * @param filePath the path to the file to be chunked.
     * @return a list of ChunkWrapper objects, each containing a chunk of the file and its name.
     * @throws IOException if an I/O error occurs reading from the file.
     */
    public static List<ChunkWrapper> chunkFile(String filePath) throws IOException {
        File file = new File(filePath);
        String baseFileName = file.getName();
        String fPath = file.getPath();
        List<ChunkWrapper> chunks = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[CHUNK_SIZE];
            int bytesRead;
            int chunkCount = 0;

            while ((bytesRead = fis.read(buffer)) != -1) {
                byte[] chunkData;
                if (bytesRead < CHUNK_SIZE) {
                    // Copy the valid bytes if the last read was smaller than CHUNK_SIZE
                    chunkData = new byte[bytesRead];
                    System.arraycopy(buffer, 0, chunkData, 0, bytesRead);
                } else {
                    // Use the full buffer if we read a full chunk
                    chunkData = buffer.clone();
                }
                String chunkName = fPath + "_chunk" + (++chunkCount);
                chunks.add(new ChunkWrapper(chunkData, chunkName, fPath));
            }
        }
        return chunks;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java FileChunker <file_path>");
            return;
        }
        FileChunker chunker = new FileChunker();
        try {
            List<ChunkWrapper> chunks = chunker.chunkFile(args[0]);
            System.out.println("Total chunks created: " + chunks.size());
            for (ChunkWrapper chunk : chunks) {
                System.out.println("Chunk " + chunk.getChunkName() + " size: " + chunk.getData().length + " bytes");
            }
        } catch (IOException e) {
            System.err.println("Error processing file: " + e.getMessage());
        }
    }
}
