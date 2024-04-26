package csx55.dfs.utils;

import csx55.dfs.config.ChunkServerConfig;
import csx55.dfs.payload.ChunkPayload;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtils {
    public static Integer getNumberOfChunks() {
        return getChunkFilesWithExtension("").size();
    }

    public static long getAvailableStorage() {
        List <Path> chunkFiles = getChunkFilesWithExtension("");
        long storageConsumed = calculateTotalSize(chunkFiles);

        return ChunkServerConfig.CHUNK_SERVER_INITIAL_FREE_SPACE - storageConsumed;
    }


    public static Integer getNumberOfChunks(String ip, Integer port) {
        return getChunkFilesWithExtension(ip + "-" + port).size();
    }

    public static List <Path> getChunkFilesWithExtension(String ip, Integer port) {
        return getChunkFilesWithExtension(ip + "-" + port);
    }

    public static long getAvailableStorage(String ip, Integer port) {
        List <Path> chunkFiles = getChunkFilesWithExtension(ip + "-" + port);
        long storageConsumed = calculateTotalSize(chunkFiles);

        return ChunkServerConfig.CHUNK_SERVER_INITIAL_FREE_SPACE - storageConsumed;
    }

    public static List<Path> getChunkFilesWithExtension(String pathAddendum) {
        Path rootPath = Paths.get(ChunkServerConfig.CHUNK_STORAGE_ROOT_DIRECTORY
         + (pathAddendum.isEmpty() ? "" : "/" + pathAddendum));
        if (!Files.exists(rootPath)) {
            try {
                Files.createDirectories(rootPath);
                System.out.println("Directory created: " + rootPath);
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                return new ArrayList<>(); // Return empty list if unable to create directory
            }
        }
        List<Path> fileList = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(rootPath)) {
            fileList = walk.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().contains(ChunkServerConfig.CHUNK_STORAGE_EXT))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
        }
        return fileList;
    }

    public static List <Path> getShardsWithExtension(String ip, Integer port) {
        return getShardsWithExtension(ip + "-" + port);
    }

    public static List<Path> getShardsWithExtension(String pathAddendum) {
        Path rootPath = Paths.get(ChunkServerConfig.CHUNK_STORAGE_ROOT_DIRECTORY
                + (pathAddendum.isEmpty() ? "" : "/" + pathAddendum));
        if (!Files.exists(rootPath)) {
            try {
                Files.createDirectories(rootPath);
                System.out.println("Directory created: " + rootPath);
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                return new ArrayList<>(); // Return empty list if unable to create directory
            }
        }
        List<Path> fileList = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(rootPath)) {
            fileList = walk.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().contains(ChunkServerConfig.SHARD_EXT))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
        }
        return fileList;
    }

    private static long calculateTotalSize(List<Path> files) {
        long totalSize = 0;
        for (Path file : files) {
            try {
                totalSize += Files.size(file);
            } catch (IOException e) {
                System.err.println("Failed to get size for file " + file + ": " + e.getMessage());
            }
        }
        return totalSize;
    }

    public static void storeFile(ChunkPayload chunkPayload, String fileStorageDirectory) {
        try {
            String chunkName = chunkPayload.getChunkWrapper().getChunkName();  // Get the full path and filename
            byte[] data = chunkPayload.getChunkWrapper().getData();  // Get the byte array data

            Path destinationPath = Paths.get(fileStorageDirectory, chunkName);  // Combine and resolve the path

            // Ensure directories exist or create them
            Files.createDirectories(destinationPath.getParent());

            // Write the byte array to the file
            Files.write(destinationPath, data);

            System.out.println("File written successfully to: " + destinationPath);
        } catch (IOException e) {
            System.err.println("Failed to write the file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
