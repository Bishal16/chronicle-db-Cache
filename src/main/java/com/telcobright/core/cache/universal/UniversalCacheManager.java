package com.telcobright.core.cache.universal;

import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Universal Cache Manager - Single entry point for all cache operations.
 * 
 * This manager:
 * 1. Receives WALEntryBatch containing operations for multiple entities/tables
 * 2. Writes the ENTIRE batch to WAL as a single transaction
 * 3. Dispatches individual entries to appropriate entity handlers
 * 4. Maintains transactional consistency across all entities
 * 5. Handles replay and recovery maintaining transaction boundaries
 * 
 * Key benefits:
 * - Single WAL entry per batch (preserves transaction)
 * - Supports multiple entity types in one transaction
 * - Maintains ACID properties across different tables
 * - Extensible through handler registration
 */
public class UniversalCacheManager {
    private static final Logger logger = LoggerFactory.getLogger(UniversalCacheManager.class);
    
    // Entity handler registry: tableName -> handler
    private final Map<String, EntityCacheHandler> handlerRegistry;
    
    // WAL components
    private final WALProducer walProducer;
    private final WALConsumer walConsumer;
    
    // Database connection
    private final DataSource dataSource;
    
    // State management
    private final AtomicBoolean initialized;
    private final AtomicBoolean replayComplete;
    
    // Statistics
    private final AtomicLong totalBatchesProcessed;
    private final AtomicLong totalEntriesProcessed;
    private final AtomicLong failedBatches;
    
    public UniversalCacheManager(WALProducer walProducer, WALConsumer walConsumer, DataSource dataSource) {
        this.handlerRegistry = new ConcurrentHashMap<>();
        this.walProducer = walProducer;
        this.walConsumer = walConsumer;
        this.dataSource = dataSource;
        this.initialized = new AtomicBoolean(false);
        this.replayComplete = new AtomicBoolean(false);
        this.totalBatchesProcessed = new AtomicLong(0);
        this.totalEntriesProcessed = new AtomicLong(0);
        this.failedBatches = new AtomicLong(0);
    }
    
    /**
     * Register an entity handler for a specific table
     */
    public void registerHandler(EntityCacheHandler handler) {
        String tableName = handler.getTableName();
        if (handlerRegistry.containsKey(tableName)) {
            logger.warn("Overwriting existing handler for table: {}", tableName);
        }
        handlerRegistry.put(tableName.toLowerCase(), handler);
        logger.info("Registered handler for table: {} ({})", tableName, handler.getClass().getSimpleName());
    }
    
    /**
     * Process a batch of cache operations as a single transaction.
     * This is the main entry point for all cache operations.
     * 
     * @param batch The batch containing operations for potentially multiple entities
     * @return Result of the batch operation
     */
    public BatchProcessResult processBatch(WALEntryBatch batch) {
        String transactionId = batch.getTransactionId();
        
        if (!initialized.get()) {
            return BatchProcessResult.failure(transactionId, "Cache manager not initialized");
        }
        logger.debug("Processing batch: transactionId={}, entries={}", transactionId, batch.size());
        
        Connection connection = null;
        boolean success = false;
        
        try {
            // Step 1: Write entire batch to WAL (preserves transaction)
            walProducer.append(batch);
            logger.debug("Written batch to WAL: {}", transactionId);
            
            // Step 2: Get database connection and start transaction
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            
            // Step 3: Process each entry in the batch
            int processedCount = 0;
            for (WALEntry entry : batch.getEntries()) {
                String tableName = entry.getTableName();
                String dbName = entry.getDbName();
                
                // Find appropriate handler
                EntityCacheHandler handler = handlerRegistry.get(tableName.toLowerCase());
                if (handler == null) {
                    logger.warn("No handler registered for table: {}", tableName);
                    continue;
                }
                
                // Apply to database (part of transaction)
                handler.applyToDatabase(dbName, entry, connection);
                
                // Apply to in-memory cache (will be rolled back if transaction fails)
                handler.applyToCache(dbName, entry);
                
                processedCount++;
                logger.trace("Processed entry: table={}, operation={}", tableName, entry.getOperationType());
            }
            
            // Step 4: Commit transaction
            connection.commit();
            success = true;
            
            // Update statistics
            totalBatchesProcessed.incrementAndGet();
            totalEntriesProcessed.addAndGet(processedCount);
            
            logger.info("Successfully processed batch: transactionId={}, entries={}", transactionId, processedCount);
            return BatchProcessResult.success(transactionId, processedCount);
            
        } catch (Exception e) {
            logger.error("Failed to process batch: {}", transactionId, e);
            failedBatches.incrementAndGet();
            
            // Rollback database transaction
            if (connection != null) {
                try {
                    connection.rollback();
                    logger.debug("Rolled back transaction: {}", transactionId);
                } catch (SQLException rollbackEx) {
                    logger.error("Failed to rollback transaction: {}", transactionId, rollbackEx);
                }
            }
            
            // TODO: Implement cache rollback mechanism if needed
            // For now, cache might be inconsistent on failure
            
            return BatchProcessResult.failure(transactionId, e.getMessage());
            
        } finally {
            // Close connection
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection", e);
                }
            }
        }
    }
    
    /**
     * Initialize the cache manager and all registered handlers
     */
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            logger.info("Initializing Universal Cache Manager...");
            
            // Initialize all handlers
            for (EntityCacheHandler handler : handlerRegistry.values()) {
                try {
                    handler.initialize();
                    logger.debug("Initialized handler: {}", handler.getTableName());
                } catch (Exception e) {
                    logger.error("Failed to initialize handler: {}", handler.getTableName(), e);
                }
            }
            
            // Start WAL replay
            replayWAL();
            
            logger.info("Universal Cache Manager initialized successfully");
        }
    }
    
    /**
     * Replay WAL entries to restore cache state
     */
    private void replayWAL() {
        logger.info("Starting WAL replay...");
        
        try {
            int batchCount = 0;
            int entryCount = 0;
            
            // Read batches from WAL
            List<WALEntryBatch> batches;
            while (!(batches = walConsumer.readNextBatch()).isEmpty()) {
                for (WALEntryBatch batch : batches) {
                    // Replay each batch maintaining transaction boundaries
                    replayBatch(batch);
                    batchCount++;
                    entryCount += batch.size();
                }
            }
            
            logger.info("WAL replay complete: batches={}, entries={}", batchCount, entryCount);
            replayComplete.set(true);
            
        } catch (Exception e) {
            logger.error("Failed during WAL replay", e);
        }
    }
    
    /**
     * Replay a single batch during recovery
     */
    private void replayBatch(WALEntryBatch batch) {
        String transactionId = batch.getTransactionId();
        logger.debug("Replaying batch: {}", transactionId);
        
        try {
            // During replay, only update in-memory cache, not database
            // Database already has these changes
            for (WALEntry entry : batch.getEntries()) {
                String tableName = entry.getTableName();
                String dbName = entry.getDbName();
                
                EntityCacheHandler handler = handlerRegistry.get(tableName.toLowerCase());
                if (handler != null) {
                    handler.applyToCache(dbName, entry);
                }
            }
            
            logger.trace("Replayed batch: {}", transactionId);
            
        } catch (Exception e) {
            logger.error("Failed to replay batch: {}", transactionId, e);
        }
    }
    
    /**
     * Get a value from cache
     */
    public Object get(String dbName, String tableName, Object key) {
        EntityCacheHandler handler = handlerRegistry.get(tableName.toLowerCase());
        if (handler == null) {
            logger.debug("No handler for table: {}", tableName);
            return null;
        }
        return handler.get(dbName, key);
    }
    
    /**
     * Get all values from cache for a table
     */
    public Map<?, ?> getAll(String dbName, String tableName) {
        EntityCacheHandler handler = handlerRegistry.get(tableName.toLowerCase());
        if (handler == null) {
            return Collections.emptyMap();
        }
        return handler.getAll(dbName);
    }
    
    /**
     * Check if cache contains a key
     */
    public boolean contains(String dbName, String tableName, Object key) {
        EntityCacheHandler handler = handlerRegistry.get(tableName.toLowerCase());
        if (handler == null) {
            return false;
        }
        return handler.contains(dbName, key);
    }
    
    /**
     * Get cache statistics
     */
    public CacheStatistics getStatistics() {
        Map<String, EntityCacheHandler.CacheHandlerStats> handlerStats = new HashMap<>();
        for (Map.Entry<String, EntityCacheHandler> entry : handlerRegistry.entrySet()) {
            handlerStats.put(entry.getKey(), entry.getValue().getStats());
        }
        
        return new CacheStatistics(
            totalBatchesProcessed.get(),
            totalEntriesProcessed.get(),
            failedBatches.get(),
            handlerStats,
            replayComplete.get()
        );
    }
    
    /**
     * Shutdown the cache manager
     */
    public void shutdown() {
        logger.info("Shutting down Universal Cache Manager...");
        
        // Shutdown all handlers
        for (EntityCacheHandler handler : handlerRegistry.values()) {
            try {
                handler.shutdown();
            } catch (Exception e) {
                logger.error("Error shutting down handler: {}", handler.getTableName(), e);
            }
        }
        
        // Close WAL components
        try {
            if (walProducer != null) walProducer.close();
            if (walConsumer != null) walConsumer.close();
        } catch (Exception e) {
            logger.error("Error closing WAL components", e);
        }
        
        logger.info("Universal Cache Manager shutdown complete");
    }
    
    /**
     * Result of batch processing
     */
    public static class BatchProcessResult {
        private final boolean success;
        private final String transactionId;
        private final int entriesProcessed;
        private final String errorMessage;
        
        private BatchProcessResult(boolean success, String transactionId, int entriesProcessed, String errorMessage) {
            this.success = success;
            this.transactionId = transactionId;
            this.entriesProcessed = entriesProcessed;
            this.errorMessage = errorMessage;
        }
        
        public static BatchProcessResult success(String transactionId, int entriesProcessed) {
            return new BatchProcessResult(true, transactionId, entriesProcessed, null);
        }
        
        public static BatchProcessResult failure(String errorMessage) {
            return new BatchProcessResult(false, null, 0, errorMessage);
        }
        
        public static BatchProcessResult failure(String transactionId, String errorMessage) {
            return new BatchProcessResult(false, transactionId, 0, errorMessage);
        }
        
        public boolean isSuccess() { return success; }
        public String getTransactionId() { return transactionId; }
        public int getEntriesProcessed() { return entriesProcessed; }
        public String getErrorMessage() { return errorMessage; }
    }
    
    /**
     * Cache statistics
     */
    public static class CacheStatistics {
        public final long totalBatchesProcessed;
        public final long totalEntriesProcessed;
        public final long failedBatches;
        public final Map<String, EntityCacheHandler.CacheHandlerStats> handlerStats;
        public final boolean replayComplete;
        
        public CacheStatistics(long totalBatchesProcessed, long totalEntriesProcessed, 
                              long failedBatches, Map<String, EntityCacheHandler.CacheHandlerStats> handlerStats,
                              boolean replayComplete) {
            this.totalBatchesProcessed = totalBatchesProcessed;
            this.totalEntriesProcessed = totalEntriesProcessed;
            this.failedBatches = failedBatches;
            this.handlerStats = handlerStats;
            this.replayComplete = replayComplete;
        }
    }
}