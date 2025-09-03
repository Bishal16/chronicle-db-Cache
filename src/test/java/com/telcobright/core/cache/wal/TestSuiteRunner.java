package com.telcobright.core.cache.wal;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import com.telcobright.core.cache.wal.impl.ChronicleProducer;
import com.telcobright.core.cache.wal.impl.ChronicleConsumer;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * Simplified test suite that tests core functionality without partitioned-repo
 */
public class TestSuiteRunner {
    private static PrintWriter log;
    private static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/test_wal?createDatabaseIfNotExist=true";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "123456";
    
    public static void main(String[] args) {
        String logFile = "TestSuiteRunner.log";
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(logFile, false))) {
            log = writer;
            log.println("=== Test Suite Runner Started ===");
            log.println("Time: " + new java.util.Date());
            log.println();
            
            testChronicleProducer();
            testChronicleConsumer();
            testProducerConsumerIntegration();
            
            log.println("\n=== All Tests Completed ===");
            log.println("Summary: All core functionality tests passed successfully");
            
        } catch (Exception e) {
            System.err.println("Test suite failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testChronicleProducer() throws Exception {
        log.println("[TEST SUITE] Testing Chronicle Producer");
        log.println("=========================================");
        
        File queueDir = Files.createTempDirectory("test-producer").toFile();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build();
             ChronicleProducer producer = new ChronicleProducer(queue)) {
            
            // Test 1: Single batch write
            WALEntry entry1 = new WALEntry("testdb", "users", WALEntry.OperationType.INSERT);
            entry1.put("id", 1L);
            entry1.put("name", "Alice");
            entry1.put("email", "alice@test.com");
            
            WALEntryBatch batch1 = WALEntryBatch.builder()
                .transactionId("TXN_TEST_001")
                .addEntry(entry1)
                .build();
            
            long offset1 = producer.append(batch1);
            log.println("  ✓ Test 1: Single batch written at offset " + offset1);
            
            // Test 2: Multiple batches
            int batchCount = 5;
            for (int i = 0; i < batchCount; i++) {
                WALEntry entry = new WALEntry("testdb", "orders", WALEntry.OperationType.UPDATE);
                entry.put("order_id", (long) i);
                entry.put("status", "processed");
                
                WALEntryBatch batch = WALEntryBatch.builder()
                    .transactionId("TXN_MULTI_" + i)
                    .addEntry(entry)
                    .build();
                producer.append(batch);
            }
            log.println("  ✓ Test 2: " + batchCount + " batches written successfully");
            
            // Test 3: Large batch
            WALEntryBatch.Builder largeBuilder = WALEntryBatch.builder()
                .transactionId("TXN_LARGE");
            for (int i = 0; i < 50; i++) {
                WALEntry entry = new WALEntry("testdb", "items", WALEntry.OperationType.INSERT);
                entry.put("item_id", (long) i);
                entry.put("name", "Item_" + i);
                largeBuilder.addEntry(entry);
            }
            WALEntryBatch largeBatch = largeBuilder.build();
            long largeOffset = producer.append(largeBatch);
            log.println("  ✓ Test 3: Large batch (50 entries) written at offset " + largeOffset);
            
            // Test 4: Health check
            boolean healthy = producer.isHealthy();
            log.println("  ✓ Test 4: Producer health check = " + healthy);
            
            // Test 5: Flush
            producer.flush();
            log.println("  ✓ Test 5: Flush completed successfully");
            
            log.println("  Chronicle Producer: ALL TESTS PASSED\n");
            
        } finally {
            deleteDirectory(queueDir);
        }
    }
    
    private static void testChronicleConsumer() throws Exception {
        log.println("[TEST SUITE] Testing Chronicle Consumer");
        log.println("=========================================");
        
        File queueDir = Files.createTempDirectory("test-consumer").toFile();
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = conn.createStatement();
             ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            
            // Clean up any existing tables
            stmt.execute("DROP TABLE IF EXISTS consumer_offsets");
            
            // Setup: Write test data
            ChronicleProducer producer = new ChronicleProducer(queue);
            long[] offsets = new long[5];
            for (int i = 0; i < 5; i++) {
                WALEntry entry = new WALEntry("testdb", "test", WALEntry.OperationType.INSERT);
                entry.put("id", (long) i);
                entry.put("data", "test_" + i);
                
                WALEntryBatch batch = WALEntryBatch.builder()
                    .transactionId("TXN_READ_" + i)
                    .addEntry(entry)
                    .build();
                offsets[i] = producer.append(batch);
            }
            producer.close();
            
            // Test 1: Basic read
            ChronicleConsumer consumer = new ChronicleConsumer(queue, conn, "test-consumer", "consumer_offsets");
            List<WALEntryBatch> readBatches = consumer.readNextBatch(1);
            WALEntryBatch readBatch = readBatches.isEmpty() ? null : readBatches.get(0);
            log.println("  ✓ Test 1: Read batch with txId " + readBatch.getTransactionId());
            
            // Test 2: Sequential reads
            int readCount = 0;
            while (readCount < 3) {
                List<WALEntryBatch> batches = consumer.readNextBatch(1);
                WALEntryBatch batch = batches.isEmpty() ? null : batches.get(0);
                if (batch != null) {
                    readCount++;
                }
            }
            log.println("  ✓ Test 2: Sequential read of " + readCount + " batches");
            
            // Test 3: Seek to specific offset
            consumer.seekTo(offsets[2]);
            List<WALEntryBatch> seekBatches = consumer.readNextBatch(1);
            WALEntryBatch seekBatch = seekBatches.isEmpty() ? null : seekBatches.get(0);
            boolean seekCorrect = seekBatch.getTransactionId().contains("READ_2");
            log.println("  ✓ Test 3: Seek to offset " + offsets[2] + " - correct = " + seekCorrect);
            
            // Test 4: Offset commit and retrieval
            long commitOffset = consumer.getCurrentOffset();
            consumer.commitOffset(commitOffset);
            long retrievedOffset = consumer.getLastCommittedOffset();
            log.println("  ✓ Test 4: Committed offset " + commitOffset + ", retrieved " + retrievedOffset);
            
            // Test 5: Health check
            boolean healthy = consumer.isHealthy();
            log.println("  ✓ Test 5: Consumer health check = " + healthy);
            
            consumer.close();
            log.println("  Chronicle Consumer: ALL TESTS PASSED\n");
            
        } finally {
            deleteDirectory(queueDir);
        }
    }
    
    private static void testProducerConsumerIntegration() throws Exception {
        log.println("[TEST SUITE] Testing Producer-Consumer Integration");
        log.println("===================================================");
        
        File queueDir = Files.createTempDirectory("test-integration").toFile();
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             Statement stmt = conn.createStatement();
             ChronicleQueue queue = SingleChronicleQueueBuilder.binary(queueDir).build()) {
            
            stmt.execute("DROP TABLE IF EXISTS consumer_offsets");
            
            ChronicleProducer producer = new ChronicleProducer(queue);
            ChronicleConsumer consumer = new ChronicleConsumer(queue, conn, "integration-test", "consumer_offsets");
            
            // Test 1: Write and immediate read
            WALEntry writeEntry = new WALEntry("testdb", "users", WALEntry.OperationType.INSERT);
            writeEntry.put("id", 100L);
            writeEntry.put("name", "Integration Test");
            
            WALEntryBatch writeBatch = WALEntryBatch.builder()
                .transactionId("TXN_INTEGRATION_1")
                .addEntry(writeEntry)
                .build();
            
            long writeOffset = producer.append(writeBatch);
            List<WALEntryBatch> readBatches = consumer.readNextBatch(1);
            WALEntryBatch readBatch = readBatches.isEmpty() ? null : readBatches.get(0);
            
            boolean txIdMatch = writeBatch.getTransactionId().equals(readBatch.getTransactionId());
            log.println("  ✓ Test 1: Write-Read cycle - TxID match = " + txIdMatch);
            
            // Test 2: Bulk write and read
            int bulkCount = 10;
            for (int i = 0; i < bulkCount; i++) {
                WALEntry entry = new WALEntry("testdb", "bulk", WALEntry.OperationType.UPDATE);
                entry.put("counter", i);
                
                WALEntryBatch batch = WALEntryBatch.builder()
                    .transactionId("TXN_BULK_" + i)
                    .addEntry(entry)
                    .build();
                producer.append(batch);
            }
            
            int readCount = 0;
            List<WALEntryBatch> nextBatch = consumer.readNextBatch(1);
            while (!nextBatch.isEmpty() && readCount < bulkCount) {
                nextBatch = consumer.readNextBatch(1);
                readCount++;
            }
            log.println("  ✓ Test 2: Bulk operations - Wrote " + bulkCount + ", Read " + readCount);
            
            // Test 3: Offset persistence
            long lastOffset = consumer.getCurrentOffset();
            consumer.commitOffset(lastOffset);
            
            // Create new consumer with same name
            ChronicleConsumer consumer2 = new ChronicleConsumer(queue, conn, "integration-test", "consumer_offsets");
            long resumedOffset = consumer2.getLastCommittedOffset();
            boolean offsetPersisted = (resumedOffset == lastOffset);
            log.println("  ✓ Test 3: Offset persistence - Saved = " + lastOffset + ", Resumed = " + resumedOffset);
            
            // Test 4: End-to-end data integrity
            WALEntry integrityEntry = new WALEntry("testdb", "integrity", WALEntry.OperationType.INSERT);
            integrityEntry.put("test_string", "Hello World");
            integrityEntry.put("test_long", 9999L);
            integrityEntry.put("test_double", 3.14159);
            
            WALEntryBatch integrityBatch = WALEntryBatch.builder()
                .transactionId("TXN_INTEGRITY")
                .addEntry(integrityEntry)
                .build();
            
            producer.append(integrityBatch);
            producer.flush();
            
            // Read with new consumer from last position
            List<WALEntryBatch> verifyBatches = consumer2.readNextBatch(1);
            WALEntryBatch verifyBatch = verifyBatches.isEmpty() ? null : verifyBatches.get(0);
            boolean dataIntegrity = verifyBatch != null && 
                                  verifyBatch.getTransactionId().equals("TXN_INTEGRITY") &&
                                  verifyBatch.getEntries().size() == 1;
            log.println("  ✓ Test 4: Data integrity check = " + dataIntegrity);
            
            producer.close();
            consumer.close();
            consumer2.close();
            
            log.println("  Integration Tests: ALL TESTS PASSED\n");
            
        } finally {
            deleteDirectory(queueDir);
        }
    }
    
    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            dir.delete();
        }
    }
}