package csx55.dfs.utils;

import erasure.ReedSolomon;

import java.io.*;


public class ReedSolomonFileDecoder {
    public static final int DATA_SHARDS = 6;
    public static final int PARITY_SHARDS = 3;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;
    public static final int BYTES_IN_INT = 4;

    public static void main(String[] args) {
        byte[][] shards = new byte[TOTAL_SHARDS][];
        boolean[] shardPresent = new boolean[TOTAL_SHARDS];
        int shardSize = 0;

        // Read shards from files
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            File shardFile = new File("shard_" + i + ".bin");
            if (shardFile.exists()) {
                try (FileInputStream inputStream = new FileInputStream(shardFile)) {
                    shards[i] = new byte[(int) shardFile.length()];
                    int bytesRead = inputStream.read(shards[i]);
                    if (bytesRead == shardFile.length()) {
                        shardPresent[i] = true;
                    } else {
                        shardPresent[i] = false; // Mark as false if file read was incomplete
                    }
                    shardSize = bytesRead;
                } catch (IOException e) {
                    System.err.println("Error reading shard " + i);
                    shardPresent[i] = false;
                    e.printStackTrace();
                }
            } else {
                shardPresent[i] = false;
            }
        }

        // Initialize missing shards to empty arrays of the correct size
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            if (!shardPresent[i]) {
                shards[i] = new byte[shardSize];
            }
        }

        // Create Reed-Solomon decoder
        ReedSolomon reedSolomon = new ReedSolomon(DATA_SHARDS, PARITY_SHARDS);
        if (!reedSolomon.isParityCorrect(shards, 0, shardSize)) {
            reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);
        }

        // Reconstruct the original text file from the data shards
        try (BufferedWriter outputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("reconstructed_file.txt"), "UTF-8"))) {
            for (int i = 0; i < DATA_SHARDS; i++) {
                if (shardPresent[i]) {
                    String textContent = new String(shards[i], i == 0 ? BYTES_IN_INT : 0, shardSize - (i == 0 ? BYTES_IN_INT : 0), "UTF-8");
                    textContent = textContent.replaceAll("\0+$", ""); // Regular expression to remove trailing NUL characters
                    outputStream.write(textContent);
                }
            }
        } catch (IOException e) {
            System.err.println("Error writing the reconstructed text file");
            e.printStackTrace();
        }
    }
}