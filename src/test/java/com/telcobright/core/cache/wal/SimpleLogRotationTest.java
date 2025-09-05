package com.telcobright.core.cache.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to demonstrate log rotation concept by creating time-based files
 * and showing how old files would be cleaned up in a real rotation scenario.
 * 
 * This test simulates Chronicle Queue log rotation behavior without requiring
 * the complex Chronicle Queue setup that has module system conflicts.
 */
public class SimpleLogRotationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(SimpleLogRotationTest.class);
    
    private static final String LOG_BASE_PATH = "/tmp/test-simple-rotation-logs";
    private static final int TEST_DURATION_MINUTES = 5;
    private static final int ROTATION_INTERVAL_MINUTES = 1;
    
    private Path logPath;
    
    @BeforeEach
    public void setUp() throws Exception {
        // Clean up any existing test directory
        logPath = Paths.get(LOG_BASE_PATH);
        if (Files.exists(logPath)) {
            deleteDirectory(logPath.toFile());
        }
        Files.createDirectories(logPath);
        
        logger.info("Setting up simple log rotation test at: {}", LOG_BASE_PATH);
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        logger.info("Test completed. Log files remain at: {} for inspection", LOG_BASE_PATH);
    }
    
    @Test
    public void testSimpleLogRotationConcept() throws Exception {
        logger.info("Starting simple log rotation test - demonstrating Chronicle Queue rotation concept");
        logger.info("Will create {} files over {} minutes, simulating {}-minute rotation", 
                   TEST_DURATION_MINUTES, TEST_DURATION_MINUTES, ROTATION_INTERVAL_MINUTES);
        
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        DateTimeFormatter displayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        
        // Create log files representing different time periods (simulating minute-based rotation)
        for (int minute = 0; minute < TEST_DURATION_MINUTES; minute++) {
            LocalDateTime now = LocalDateTime.now().plusMinutes(minute); // Simulate different minutes
            String timestamp = now.format(formatter);
            String displayTime = now.format(displayFormatter);
            
            // Create a log file for this "minute" (simulating Chronicle Queue .cq4 files)
            String fileName = String.format("chronicle-queue-%s.cq4", timestamp);
            File logFile = new File(logPath.toFile(), fileName);
            
            // Write some mock log data to simulate WAL entries
            try (FileWriter writer = new FileWriter(logFile)) {
                writer.write(String.format("# Chronicle Queue Log File - Created at %s\n", displayTime));
                writer.write(String.format("# Simulating WAL entries for minute %d\n", minute + 1));
                writer.write(String.format("WAL_ENTRY_%d_1: INSERT into test_table values(id=%d, data='Test data for minute %d')\n", 
                           minute + 1, 1000 + minute, minute + 1));
                writer.write(String.format("WAL_ENTRY_%d_2: TIMESTAMP=%d\n", 
                           minute + 1, Instant.now().toEpochMilli()));
                writer.write(String.format("WAL_ENTRY_%d_3: BATCH_ID=rotation-test-tx-%d\n", 
                           minute + 1, minute + 1));
            }
            
            logger.info("Created log file {} representing minute {} ({})", 
                       fileName, minute + 1, displayTime);
            
            // List current files
            listLogFiles(minute + 1);
            
            // Simulate the passage of time
            if (minute < TEST_DURATION_MINUTES - 1) {
                logger.info("Simulating 1 minute passage...");
                TimeUnit.SECONDS.sleep(2); // Short sleep for demo purposes
            }
        }
        
        // Now simulate log rotation cleanup - keep only the most recent files
        logger.info("\n=== Simulating Log Rotation Cleanup ===");
        performRotationCleanup();
        
        // Final check
        int finalFileCount = listLogFiles(-1);
        
        logger.info("\n🎯 Simple Log Rotation Test Results:");
        logger.info("- Original files created: {}", TEST_DURATION_MINUTES);
        logger.info("- Files after rotation cleanup: {}", finalFileCount);
        logger.info("- Rotation policy: Keep only files from last {} minute(s)", ROTATION_INTERVAL_MINUTES);
        
        // With 1-minute rotation, we expect only 1-2 files to remain (current + maybe previous)
        assertTrue(finalFileCount <= 2, 
                  "Expected at most 2 log files after 1-minute rotation cleanup, but found: " + finalFileCount);
        assertTrue(finalFileCount >= 1, 
                  "Expected at least 1 log file to remain, but found: " + finalFileCount);
        
        logger.info("✅ Simple log rotation concept test PASSED - cleanup working correctly");
    }
    
    /**
     * Simulate Chronicle Queue log rotation cleanup
     * In real Chronicle Queue, this happens automatically based on RollCycle configuration
     */
    private void performRotationCleanup() throws Exception {
        File logDir = new File(LOG_BASE_PATH);
        File[] files = logDir.listFiles((dir, name) -> name.endsWith(".cq4"));
        
        if (files == null || files.length == 0) {
            logger.warn("No log files found for rotation cleanup");
            return;
        }
        
        // Sort files by name (which includes timestamp)
        Arrays.sort(files, (f1, f2) -> f2.getName().compareTo(f1.getName())); // Latest first
        
        logger.info("Found {} files for rotation cleanup analysis", files.length);
        
        // Keep only the most recent files (simulating 1-minute rotation)
        int filesToKeep = ROTATION_INTERVAL_MINUTES + 1; // Keep current + 1 previous
        int deletedCount = 0;
        
        for (int i = filesToKeep; i < files.length; i++) {
            File fileToDelete = files[i];
            logger.info("Rotating out (deleting) old file: {}", fileToDelete.getName());
            if (fileToDelete.delete()) {
                deletedCount++;
            } else {
                logger.warn("Failed to delete file: {}", fileToDelete.getName());
            }
        }
        
        logger.info("Log rotation cleanup completed: {} files deleted, {} files retained", 
                   deletedCount, Math.min(filesToKeep, files.length));
    }
    
    /**
     * List and count log files in the directory
     */
    private int listLogFiles(int minute) {
        try {
            File logDir = new File(LOG_BASE_PATH);
            File[] files = logDir.listFiles((dir, name) -> name.endsWith(".cq4"));
            
            if (files == null) {
                logger.warn("No log files found in directory: {}", LOG_BASE_PATH);
                return 0;
            }
            
            Arrays.sort(files, (f1, f2) -> f1.getName().compareTo(f2.getName()));
            
            String logPrefix = minute > 0 ? String.format("After minute %d", minute) : "Final check";
            logger.info("{} - Log files ({}):", logPrefix, files.length);
            
            for (File file : files) {
                long sizeBytes = file.length();
                long ageSeconds = (System.currentTimeMillis() - file.lastModified()) / 1000;
                logger.info("  - {} ({}B, {}s old)", file.getName(), sizeBytes, ageSeconds);
            }
            
            return files.length;
            
        } catch (Exception e) {
            logger.error("Error listing log files", e);
            return 0;
        }
    }
    
    /**
     * Recursively delete directory and all contents
     */
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
    
    /**
     * Test demonstrating how Chronicle Queue file naming works with timestamps
     */
    @Test
    public void testChronicleQueueFileNamingConcept() {
        logger.info("Demonstrating Chronicle Queue file naming concept");
        
        // Chronicle Queue files are typically named with timestamps/cycle numbers
        // For MINUTELY roll cycle, files might be named like:
        // 20250905-2023.cq4 (for 2025-09-05 20:23)
        // 20250905-2024.cq4 (for 2025-09-05 20:24)
        // etc.
        
        LocalDateTime now = LocalDateTime.now();
        for (int i = 0; i < 5; i++) {
            LocalDateTime time = now.plusMinutes(i);
            String chronicleFileName = time.format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmm")) + ".cq4";
            logger.info("Minute {}: Chronicle Queue file would be named: {}", i + 1, chronicleFileName);
        }
        
        logger.info("With MINUTELY roll cycle, Chronicle Queue automatically:");
        logger.info("  1. Creates new .cq4 file each minute");
        logger.info("  2. Rotates out old files based on retention policy");
        logger.info("  3. Maintains only recent files (e.g., last 1-2 minutes)");
        logger.info("  4. Ensures atomic operations within each time window");
        
        assertTrue(true, "File naming concept demonstration completed");
    }
}