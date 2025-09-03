package com.telcobright.examples;

import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.processor.BatchProcessor;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Database cache implementation with batch processing.
 * Configurable batch size with atomic transaction handling.
 */
public class DBCacheWithBatching<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(DBCacheWithBatching.class);
    
    private final ConcurrentHashMap<K, V> cache;
    private final WALProducer producer;
    private final WALConsumer consumer;
    private final BatchProcessor batchProcessor;
    private final Connection dbConnection;
    private final ExecutorService replayExecutor;
    private final AtomicBoolean replayComplete;
    private final int batchSize;
    
    /**
     * Creates a DB cache with default batch size of 100.
     */
    public DBCacheWithBatching(WALProducer producer, WALConsumer consumer, 
                              Connection dbConnection) {
        this(producer, consumer, dbConnection, 100);
    }
    
    /**
     * Creates a DB cache with custom batch size.
     * 
     * @param producer WAL producer for writing entries
     * @param consumer WAL consumer for reading entries
     * @param dbConnection Database connection for persistent storage
     * @param batchSize Number of entries to process in each batch (minimum 1)
     */
    public DBCacheWithBatching(WALProducer producer, WALConsumer consumer,
                              Connection dbConnection, int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be at least 1, got: " + batchSize);
        }
        
        this.cache = new ConcurrentHashMap<>();
        this.producer = producer;
        this.consumer = consumer;
        this.dbConnection = dbConnection;
        this.batchSize = batchSize;
        this.replayExecutor = Executors.newSingleThreadExecutor();
        this.replayComplete = new AtomicBoolean(false);
        
        // Configure consumer batch size
        consumer.setBatchSize(batchSize);
        
        // Create batch processor with custom entry handler
        this.batchProcessor = new BatchProcessor(consumer, dbConnection, this::applyEntryToDB, batchSize);
        
        // Start initialization
        initialize();
    }
    
    /**
     * Initializes the cache by:
     * 1. Loading initial data from database
     * 2. Replaying WAL entries to catch up
     * 3. Blocking new writes until replay is complete
     */
    private void initialize() {
        logger.info("Initializing DB cache with batch size: {}", batchSize);
        
        // Load initial data from database
        loadInitialData();
        
        // Start replay in background
        replayExecutor.submit(() -> {
            try {
                logger.info("Starting WAL replay...");
                long startOffset = consumer.getLastCommittedOffset();
                logger.info("Replaying from offset: {}", startOffset);
                
                // Process all pending batches
                int totalProcessed = 0;
                int processed;
                do {
                    processed = batchProcessor.processNextBatch();
                    totalProcessed += processed;
                    
                    if (processed > 0) {
                        logger.debug("Processed batch of {} entries", processed);
                    }
                } while (processed > 0);
                
                logger.info("WAL replay complete. Total entries processed: {}", totalProcessed);
                replayComplete.set(true);
                
                // Continue processing new entries
                processNewEntries();
                
            } catch (Exception e) {
                logger.error("Failed during WAL replay", e);
            }
        });
    }
    
    /**
     * Loads initial data from database into cache.
     */
    private void loadInitialData() {
        logger.info("Loading initial data from database...");
        // Implementation depends on specific table structure
        // This is a placeholder for the actual loading logic
    }
    
    /**
     * Applies a WAL entry to the database.
     * Called by BatchProcessor for each entry in a transaction.
     */
    private void applyEntryToDB(WALEntry entry, Connection conn) {
        try {
            WALEntry.OperationType operation = entry.getOperationType();
            
            switch (operation) {
                case INSERT:
                case UPDATE:
                case UPSERT:
                    upsertEntry(entry, conn);
                    break;
                case DELETE:
                    deleteEntry(entry, conn);
                    break;
                default:
                    logger.warn("Unknown operation: {}", operation);
            }
            
            // Update cache after DB operation
            updateCache(entry);
            
        } catch (Exception e) {
            logger.error("Failed to apply entry to DB: {}", entry, e);
            throw new RuntimeException("Failed to apply entry", e);
        }
    }
    
    private void upsertEntry(WALEntry entry, Connection conn) throws Exception {
        // Get key and value from data map
        Object key = entry.get("id");
        Object value = entry.get("value");
        
        String sql = "INSERT INTO " + entry.getTableName() + " (id, data) VALUES (?, ?) " +
                    "ON DUPLICATE KEY UPDATE data = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, key);
            ps.setObject(2, value);
            ps.setObject(3, value);
            ps.executeUpdate();
        }
    }
    
    private void deleteEntry(WALEntry entry, Connection conn) throws Exception {
        Object key = entry.get("id");
        String sql = "DELETE FROM " + entry.getTableName() + " WHERE id = ?";
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, key);
            ps.executeUpdate();
        }
    }
    
    private void updateCache(WALEntry entry) {
        WALEntry.OperationType operation = entry.getOperationType();
        K key = (K) entry.get("id");
        
        switch (operation) {
            case INSERT:
            case UPDATE:
            case UPSERT:
                V value = (V) entry.get("value");
                if (key != null && value != null) {
                    cache.put(key, value);
                }
                break;
            case DELETE:
                if (key != null) {
                    cache.remove(key);
                }
                break;
            default:
                break;
        }
    }
    
    /**
     * Continuously processes new entries after replay is complete.
     */
    private void processNewEntries() {
        try {
            logger.info("Starting continuous processing of new entries...");
            
            batchProcessor.processContinuously(() -> Thread.currentThread().isInterrupted());
            
        } catch (Exception e) {
            logger.error("Error during continuous processing", e);
        }
    }
    
    /**
     * Gets a value from cache. Blocks if replay is not complete.
     */
    public V get(K key) {
        waitForReplay();
        return cache.get(key);
    }
    
    /**
     * Puts a value to cache and WAL. Blocks if replay is not complete.
     */
    public void put(K key, V value) throws Exception {
        waitForReplay();
        
        // Write to WAL first
        WALEntry entry = WALEntry.builder()
            .operationType(WALEntry.OperationType.UPSERT)
            .tableName("entities")
            .withData("id", key)
            .withData("value", value)
            .build();
        
        // Producer handles the actual batch creation
        // Transaction ID is auto-generated internally
        WALEntryBatch batch = WALEntryBatch.builder()
            .addEntry(entry)
            .build(); // Transaction ID auto-generated in build()
        producer.append(batch);
        
        // Update cache optimistically
        cache.put(key, value);
    }
    
    /**
     * Blocks until WAL replay is complete.
     */
    private void waitForReplay() {
        if (!replayComplete.get()) {
            logger.debug("Waiting for WAL replay to complete...");
            while (!replayComplete.get()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
    
    /**
     * Shuts down the cache and releases resources.
     */
    public void shutdown() {
        try {
            replayExecutor.shutdown();
            producer.close();
            consumer.close();
            dbConnection.close();
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }
}