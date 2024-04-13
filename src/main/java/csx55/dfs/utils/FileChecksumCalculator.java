package csx55.dfs.utils;

import csx55.dfs.config.ChunkServerConfig;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FileChecksumCalculator {

    private static final int MAX_SLICE_SIZE = ChunkServerConfig.MAX_SLICE_SIZE; // 8KB slices

    private static Map<String, List<String>> initialChecksums = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {

        initialChecksums = getChecksumMapForChunkInDirectory(ChunkServerConfig.CHUNK_STORAGE_ROOT_DIRECTORY);
        Runnable checksumVerificationTask = () -> {
            Map<String, List<String>> currentCheckSums = getChecksumMapForChunkInDirectory(ChunkServerConfig.CHUNK_STORAGE_ROOT_DIRECTORY);
            System.out.println("Scheduled checksum calculation completed. Total files processed: " + currentCheckSums.size());
            verifyChecksums(currentCheckSums);
        };

        // Schedule the task to run every 15 seconds
        scheduler.scheduleAtFixedRate(checksumVerificationTask, 0, 15, TimeUnit.SECONDS);
    }

    public static Map <String, List <Integer>> verifyChecksums(Map<String, List<String>> currentChecksums) {
        Map <String, List <Integer>> checksumViolationMap = new HashMap<>();
        currentChecksums.forEach((filePath, newChecksums) -> {
            List<String> oldChecksums = initialChecksums.get(filePath);
            if (oldChecksums == null) {
                System.out.println("New file detected: " + filePath);
                initialChecksums.put(filePath, new ArrayList<>(newChecksums));
                System.out.println("Checksums for new file added to initial checksums map.");
            } else {
                boolean mismatchFound = false;
                List <Integer> violationSlices = new ArrayList<>();
                for (int i = 0; i < newChecksums.size(); i++) {
                    if (!newChecksums.get(i).equals(oldChecksums.get(i))) {
                        System.out.printf("Checksum mismatch detected in %s at slice %d%n", filePath, i + 1);
                        mismatchFound = true;
                        violationSlices.add(i);
                        checksumViolationMap.put(filePath, violationSlices);
                        if (checksumViolationMap.containsKey(filePath)) {
                            checksumViolationMap.replace(filePath, violationSlices);
                        }
                    }
                }
                if (!mismatchFound) {
                    System.out.printf("No checksum mismatch detected for file %s%n", filePath);
                    checksumViolationMap.put(filePath, violationSlices);
                }
            }
        });
        return checksumViolationMap;
    }


    public static Map<String, List <String>> getChecksumMapForChunkInDirectory (String storageRoot) {
        Map <String, List <String>> checksumMap = new HashMap<>();
        Path rootDirectory = Paths.get(storageRoot);
        try {
            List<Path> files = Files.walk(rootDirectory)
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().contains(ChunkServerConfig.CHUNK_STORAGE_EXT))
                    .collect(Collectors.toList());

            for (Path file : files) {
                byte[] fileData = Files.readAllBytes(file);
                List<String> checksums = computeChecksums(fileData);
                checksumMap.put(String.valueOf(file.getFileName()), checksums);
                System.out.println("Checksums for file: " + file.getFileName());
                for (int i = 0; i < checksums.size(); i++) {
                    System.out.println(" Slice " + (i + 1) + ": " + checksums.get(i));
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading files: " + e.getMessage());
        }
        return checksumMap;
    }

    public static List<String> computeChecksums(byte[] fileData) {
        List<String> checksums = new ArrayList<>();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");

            int sliceSize;
            for (int start = 0; start < fileData.length; start += sliceSize) {
                sliceSize = Math.min(MAX_SLICE_SIZE, fileData.length - start);
                digest.reset();
                digest.update(fileData, start, sliceSize);
                byte[] hashBytes = digest.digest();
                checksums.add(bytesToHex(hashBytes));
            }

        } catch (NoSuchAlgorithmException e) {
            System.err.println("SHA-1 algorithm not available: " + e.getMessage());
            return null;
        }

        return checksums;
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
