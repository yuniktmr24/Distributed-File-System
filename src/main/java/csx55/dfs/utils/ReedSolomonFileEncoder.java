package csx55.dfs.utils;



import erasure.ReedSolomon;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ReedSolomonFileEncoder {

    public static final int DATA_SHARDS = 6;
    public static final int PARITY_SHARDS = 3;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;
    public static final int BYTES_IN_INT = 4;

    public static void main(String[] args) {
        File inputFile = new File("src/main/resources/input.txt");
        byte[][] shards;
        int shardSize = 0;

        // Calculate shard size and prepare buffer
        try (FileInputStream inputStream = new FileInputStream(inputFile)) {
            int fileSize = (int) inputFile.length();
            int storedSize = fileSize + BYTES_IN_INT;
            shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;
            int bufferSize = shardSize * DATA_SHARDS;
            byte[] allBytes = new byte[bufferSize];
            ByteBuffer.wrap(allBytes).putInt(fileSize);
            inputStream.read(allBytes, BYTES_IN_INT, fileSize);

            // Padding for alignment
            for (int i = fileSize + BYTES_IN_INT; i < allBytes.length; i++) {
                allBytes[i] = 0;
            }

            // Divide data into shards
            shards = new byte[TOTAL_SHARDS][shardSize];
            for (int i = 0; i < DATA_SHARDS; i++) {
                System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
            }
        } catch (IOException e) {
            System.err.println("Error reading input file");
            e.printStackTrace();
            return;
        }

        // Create and use Reed-Solomon to calculate the parity
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);

        // Save shards to disk
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            File outputFile = new File("shard_" + i + ".bin");
            try (FileOutputStream out = new FileOutputStream(outputFile)) {
                out.write(shards[i]);
            } catch (IOException e) {
                System.err.println("Error writing shard file " + outputFile.getName());
                e.printStackTrace();
            }
        }
    }
}

