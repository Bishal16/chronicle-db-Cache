package com.telcobright.core.cache.wal;

import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.impl.ChronicleProducer;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
 * Test class specifically for Chronicle Queue log rotation functionality.
 * Tests 1-minute rotation cycle by writing entries over 5 minutes and verifying
 * that only files from the last minute remain due to rotation.
 */
public class ChronicleQueueLogRotationTest {
    
    private static final Logger logger = LoggerFactory.getLogger(ChronicleQueueLogRotationTest.class);
    
    private static final String QUEUE_BASE_PATH = "/tmp/test-chronicle-rotation-queue";
    private static final int TEST_DURATION_MINUTES = 5;
    private static final int ROTATION_INTERVAL_MINUTES = 1;
    
    private ChronicleQueue queue;
    private WALProducer walProducer;
    private Path queuePath;
    
    @BeforeEach
    public void setUp() throws Exception {
        // Clean up any existing test directory
        queuePath = Paths.get(QUEUE_BASE_PATH);
        if (Files.exists(queuePath)) {
            deleteDirectory(queuePath.toFile());
        }
        Files.createDirectories(queuePath);
        
        logger.info("Setting up Chronicle Queue with 1-minute rotation at: {}", QUEUE_BASE_PATH);
        
        // Create Chronicle Queue with 1-minute rotation using builder syntax
        queue = ChronicleQueue.singleBuilder(QUEUE_BASE_PATH)
                .rollCycle(RollCycles.MINUTELY) // 1-minute rotation
                .build();
        
        // Create WAL producer using ChronicleProducer
        walProducer = new ChronicleProducer(queue);
        
        logger.info("Chronicle Queue initialized with MINUTELY roll cycle");
    }
    
    @AfterEach
    public void tearDown() throws Exception {
        if (queue != null) {
            queue.close();
        }
        if (walProducer != null) {
            walProducer.close();
        }
        
        // Keep test files for manual inspection
        logger.info("Test completed. Queue files remain at: {} for inspection", QUEUE_BASE_PATH);
    }
    
    @Test
    public void testLogRotationWithOneMinuteInterval() throws Exception {
        logger.info("Starting log rotation test - will run for {} minutes with {}-minute rotation", 
                   TEST_DURATION_MINUTES, ROTATION_INTERVAL_MINUTES);
        
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        
        // Write one entry per minute for 5 minutes
        for (int minute = 0; minute < TEST_DURATION_MINUTES; minute++) {
            LocalDateTime now = LocalDateTime.now();
            String timestamp = now.format(formatter);
            
            // Create a WAL entry using proper API
            WALEntry entry = WALEntry.builder()
                    .dbName("testdb")
                    .tableName("test_table_" + minute)
                    .operationType(WALEntry.OperationType.INSERT)
                    .withData("id", (long) (1000 + minute))
                    .withData("data", String.format("Test data for minute %d at %s", minute + 1, timestamp))
                    .withData("minute", minute + 1)
                    .build();
            
            entry.setTimestamp(Instant.now().toEpochMilli());
            
            // Create a batch with single entry and write to queue
            WALEntryBatch batch = WALEntryBatch.builder()
                    .transactionId("rotation-test-tx-" + (minute + 1))
                    .addEntry(entry)
                    .build();
            
            walProducer.append(batch);
            
            logger.info("Written entry for table {} at minute {} ({})", 
                       entry.getTableName(), minute + 1, timestamp);
            
            // List current queue files
            listQueueFiles(minute + 1);
            
            // Wait for 1 minute before next write (except for the last iteration)
            if (minute < TEST_DURATION_MINUTES - 1) {
                logger.info("Waiting 1 minute before next entry...");
                TimeUnit.MINUTES.sleep(1);
            }
        }
        
        // Wait a bit more to ensure rotation has time to complete
        logger.info("Waiting additional 30 seconds for final rotation...");
        TimeUnit.SECONDS.sleep(30);
        
        // Final check of queue files
        int finalFileCount = listQueueFiles(-1);
        
        logger.info("Test completed. Final analysis:");
        logger.info("- Total test duration: {} minutes", TEST_DURATION_MINUTES);
        logger.info("- Rotation interval: {} minute(s)", ROTATION_INTERVAL_MINUTES);
        logger.info("- Final file count: {}", finalFileCount);
        
        // With 1-minute rotation, we expect only the most recent file(s) to remain
        // Chronicle Queue typically keeps current + previous file, so 1-2 files expected
        assertTrue(finalFileCount <= 3, 
                  "Expected at most 3 queue files due to 1-minute rotation, but found: " + finalFileCount);
        assertTrue(finalFileCount >= 1, 
                  "Expected at least 1 queue file to exist, but found: " + finalFileCount);
        
        logger.info("Log rotation test PASSED - file rotation working correctly");
    }
    
    /**
     * List and count queue files in the directory
     */
    private int listQueueFiles(int minute) {
        try {
            File queueDir = new File(QUEUE_BASE_PATH);
            File[] files = queueDir.listFiles((dir, name) -> 
                name.endsWith(".cq4") || name.endsWith(".cq4t"));
            
            if (files == null) {
                logger.warn("No queue files found in directory: {}", QUEUE_BASE_PATH);
                return 0;
            }
            
            Arrays.sort(files, (f1, f2) -> f1.getName().compareTo(f2.getName()));
            
            String logPrefix = minute > 0 ? String.format("After minute %d", minute) : "Final check";
            logger.info("{} - Queue files ({}):", logPrefix, files.length);
            
            for (File file : files) {
                long sizeKB = file.length() / 1024;
                long ageSeconds = (System.currentTimeMillis() - file.lastModified()) / 1000;
                logger.info("  - {} ({}KB, {}s old)", file.getName(), sizeKB, ageSeconds);
            }
            
            return files.length;
            
        } catch (Exception e) {
            logger.error("Error listing queue files", e);
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
     * Additional test method to verify rotation behavior in detail
     */
    @Test
    public void testRotationBehaviorDetailed() throws Exception {
        logger.info("Starting detailed rotation behavior test");
        
        // Write entries more frequently to better observe rotation
        for (int i = 0; i < 10; i++) {
            WALEntry entry = WALEntry.builder()
                    .dbName("detaileddb")
                    .tableName("detailed_table_" + i)
                    .operationType(WALEntry.OperationType.UPDATE)
                    .withData("id", (long) (2000 + i))
                    .withData("data", String.format("Detailed test entry %d at %s", i, 
                          LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))))
                    .build();
            
            entry.setTimestamp(Instant.now().toEpochMilli());
            
            WALEntryBatch detailedBatch = WALEntryBatch.builder()
                    .transactionId("detailed-test-tx-" + (i + 1))
                    .addEntry(entry)
                    .build();
            
            walProducer.append(detailedBatch);
            logger.info("Written detailed entry {}", i + 1);
            
            // Wait 30 seconds between entries
            TimeUnit.SECONDS.sleep(30);
            
            // Check files every few entries
            if ((i + 1) % 3 == 0) {
                listQueueFiles(i + 1);
            }
        }
        
        // Final file count check
        int fileCount = listQueueFiles(-1);
        logger.info("Detailed test completed with {} files remaining", fileCount);
        
        // Should still have limited files due to rotation
        assertTrue(fileCount <= 4, "Too many files remaining after detailed test: " + fileCount);
    }
}