package com.telcobright.core.cache;

import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.cache.wal.processor.BatchProcessor;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.PrintWriter;
import java.io.FileWriter;

/**
 * Test for batch processing functionality.
 */
public class BatchProcessingTest {
    
    private Connection connection;
    private PrintWriter logWriter;
    
    @BeforeEach
    void setUp() throws Exception {
        // Setup database connection
        connection = DriverManager.getConnection(
            "jdbc:mysql://127.0.0.1:3306/test_db", "root", "123456");
        
        // Create test table
        connection.createStatement().execute(
            "CREATE TABLE IF NOT EXISTS test_entities (" +
            "id INT PRIMARY KEY, " +
            "value VARCHAR(255)" +
            ")");
        
        // Clear table
        connection.createStatement().execute("TRUNCATE TABLE test_entities");
        
        // Setup log writer
        logWriter = new PrintWriter(new FileWriter("BatchProcessingTest.log", true));
        logWriter.println("\n=== BatchProcessingTest Started at " + 
                         new java.util.Date() + " ===");
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        if (logWriter != null) {
            logWriter.println("=== Test Completed ===\n");
            logWriter.close();
        }
    }
    
    @Test
    void testBatchProcessingWithDefaultSize() throws Exception {
        logWriter.println("\nTest: Batch Processing with Default Size (100)");
        
        // Create mock consumer
        MockWALConsumer consumer = new MockWALConsumer();
        
        // Add test data
        for (int i = 0; i < 150; i++) {
            WALEntry entry = WALEntry.builder()
                .tableName("test_entities")
                .operationType(WALEntry.OperationType.INSERT)
                .withData("id", i)
                .withData("value", "value" + i)
                .build();
            
            WALEntryBatch batch = WALEntryBatch.single("TXN_" + i, entry);
            consumer.addBatch(batch);
        }
        
        // Create processor with custom entry handler
        AtomicInteger processedCount = new AtomicInteger(0);
        BatchProcessor processor = new BatchProcessor(
            consumer, 
            connection,
            (entry, conn) -> {
                try {
                    String sql = "INSERT INTO test_entities (id, value) VALUES (?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, (Integer) entry.get("id"));
                        ps.setString(2, (String) entry.get("value"));
                        ps.executeUpdate();
                    }
                    processedCount.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        
        // Process first batch (should process 100)
        int processed = processor.processNextBatch();
        assertEquals(100, processed);
        assertEquals(100, processedCount.get());
        logWriter.println("First batch processed: " + processed + " entries");
        
        // Verify data in database
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT COUNT(*) FROM test_entities");
        rs.next();
        assertEquals(100, rs.getInt(1));
        logWriter.println("Database contains: " + rs.getInt(1) + " entries");
        
        // Process remaining (should process 50)
        processed = processor.processNextBatch();
        assertEquals(50, processed);
        assertEquals(150, processedCount.get());
        logWriter.println("Second batch processed: " + processed + " entries");
        
        // Verify all data
        rs = connection.createStatement().executeQuery(
            "SELECT COUNT(*) FROM test_entities");
        rs.next();
        assertEquals(150, rs.getInt(1));
        logWriter.println("Final database count: " + rs.getInt(1) + " entries");
        
        logWriter.println("✓ Test passed: Default batch size processing");
    }
    
    @Test
    void testBatchProcessingWithCustomSize() throws Exception {
        logWriter.println("\nTest: Batch Processing with Custom Size");
        
        MockWALConsumer consumer = new MockWALConsumer();
        
        // Add 25 test entries
        for (int i = 0; i < 25; i++) {
            WALEntry entry = WALEntry.builder()
                .tableName("test_entities")
                .operationType(WALEntry.OperationType.INSERT)
                .withData("id", i)
                .withData("value", "value" + i)
                .build();
            
            WALEntryBatch batch = WALEntryBatch.single("TXN_" + i, entry);
            consumer.addBatch(batch);
        }
        
        BatchProcessor processor = new BatchProcessor(
            consumer,
            connection,
            (entry, conn) -> {
                try {
                    String sql = "INSERT INTO test_entities (id, value) VALUES (?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, (Integer) entry.get("id"));
                        ps.setString(2, (String) entry.get("value"));
                        ps.executeUpdate();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            10  // Custom batch size of 10
        );
        
        // Process with batch size 10
        int processed = processor.processNextBatch();
        assertEquals(10, processed);
        logWriter.println("Batch 1: " + processed + " entries");
        
        processed = processor.processNextBatch();
        assertEquals(10, processed);
        logWriter.println("Batch 2: " + processed + " entries");
        
        processed = processor.processNextBatch();
        assertEquals(5, processed);
        logWriter.println("Batch 3: " + processed + " entries");
        
        // No more data
        processed = processor.processNextBatch();
        assertEquals(0, processed);
        logWriter.println("Batch 4: " + processed + " entries (empty)");
        
        logWriter.println("✓ Test passed: Custom batch size processing");
    }
    
    @Test
    void testAtomicTransactionRollback() throws Exception {
        logWriter.println("\nTest: Atomic Transaction Rollback");
        
        MockWALConsumer consumer = new MockWALConsumer();
        
        // Add entries including one that will fail
        for (int i = 0; i < 10; i++) {
            WALEntry entry = WALEntry.builder()
                .tableName("test_entities")
                .operationType(WALEntry.OperationType.INSERT)
                .withData("id", i)
                .withData("value", i == 5 ? null : "value" + i) // null will cause failure
                .build();
            
            WALEntryBatch batch = WALEntryBatch.single("TXN_" + i, entry);
            consumer.addBatch(batch);
        }
        
        AtomicInteger successCount = new AtomicInteger(0);
        BatchProcessor processor = new BatchProcessor(
            consumer,
            connection,
            (entry, conn) -> {
                try {
                    String sql = "INSERT INTO test_entities (id, value) VALUES (?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, (Integer) entry.get("id"));
                        String value = (String) entry.get("value");
                        if (value == null) {
                            throw new RuntimeException("Null value not allowed");
                        }
                        ps.setString(2, value);
                        ps.executeUpdate();
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            10
        );
        
        // Process should fail and rollback
        try {
            processor.processNextBatch();
            fail("Should have thrown exception");
        } catch (Exception e) {
            logWriter.println("Expected exception: " + e.getMessage());
        }
        
        // Verify rollback - no data should be in database
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT COUNT(*) FROM test_entities");
        rs.next();
        assertEquals(0, rs.getInt(1));
        logWriter.println("Database count after rollback: " + rs.getInt(1));
        logWriter.println("Successful operations before rollback: " + successCount.get());
        
        logWriter.println("✓ Test passed: Transaction rolled back atomically");
    }
    
    @Test
    void testBatchSizeOfOne() throws Exception {
        logWriter.println("\nTest: Batch Size of 1 (Single Entry Processing)");
        
        MockWALConsumer consumer = new MockWALConsumer();
        
        // Add 5 entries
        for (int i = 0; i < 5; i++) {
            WALEntry entry = WALEntry.builder()
                .tableName("test_entities")
                .operationType(WALEntry.OperationType.INSERT)
                .withData("id", i)
                .withData("value", "value" + i)
                .build();
            
            WALEntryBatch batch = WALEntryBatch.single("TXN_" + i, entry);
            consumer.addBatch(batch);
        }
        
        BatchProcessor processor = new BatchProcessor(
            consumer,
            connection,
            (entry, conn) -> {
                try {
                    String sql = "INSERT INTO test_entities (id, value) VALUES (?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, (Integer) entry.get("id"));
                        ps.setString(2, (String) entry.get("value"));
                        ps.executeUpdate();
                    }
                    logWriter.println("  Processed entry: id=" + entry.get("id"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            },
            1  // Batch size of 1
        );
        
        // Process entries one by one
        for (int i = 0; i < 5; i++) {
            int processed = processor.processNextBatch();
            assertEquals(1, processed);
            logWriter.println("Batch " + (i+1) + ": " + processed + " entry");
        }
        
        // Verify all data
        ResultSet rs = connection.createStatement().executeQuery(
            "SELECT COUNT(*) FROM test_entities");
        rs.next();
        assertEquals(5, rs.getInt(1));
        
        logWriter.println("✓ Test passed: Single entry batch processing");
    }
    
    /**
     * Mock WAL Consumer for testing
     */
    private static class MockWALConsumer implements WALConsumer {
        private final List<WALEntryBatch> batches = new ArrayList<>();
        private int currentIndex = 0;
        private int batchSize = 100;
        private long lastCommittedOffset = -1;
        
        public void addBatch(WALEntryBatch batch) {
            batches.add(batch);
        }
        
        @Override
        public List<WALEntryBatch> readNextBatch() throws Exception {
            return readNextBatch(batchSize);
        }
        
        @Override
        public List<WALEntryBatch> readNextBatch(int size) throws Exception {
            List<WALEntryBatch> result = new ArrayList<>();
            
            for (int i = 0; i < size && currentIndex < batches.size(); i++) {
                result.add(batches.get(currentIndex++));
            }
            
            return result;
        }
        
        @Override
        public int getBatchSize() {
            return batchSize;
        }
        
        @Override
        public void setBatchSize(int size) {
            this.batchSize = size;
        }
        
        @Override
        public void seekTo(long offset) throws Exception {
            currentIndex = (int) offset;
        }
        
        @Override
        public long getCurrentOffset() {
            return currentIndex - 1;
        }
        
        @Override
        public void commitOffset(long offset) throws Exception {
            lastCommittedOffset = offset;
        }
        
        @Override
        public long getLastCommittedOffset() throws Exception {
            return lastCommittedOffset;
        }
        
        @Override
        public boolean isHealthy() {
            return true;
        }
        
        @Override
        public boolean handleCorruption() throws Exception {
            return false;
        }
        
        @Override
        public void close() {
            // Nothing to close
        }
    }
}