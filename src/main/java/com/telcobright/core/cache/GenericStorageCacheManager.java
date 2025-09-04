package com.telcobright.core.cache;

import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.db.genericentity.api.GenericEntityStorage;
import com.telcobright.core.cache.entity.CacheEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache Manager using GenericEntityStorage for unified multi-entity storage.
 * This implementation enables true atomic batch operations across multiple entity types
 * by storing all entities in a single GenericEntityStorage instance.
 */
public class GenericStorageCacheManager {
    private static final Logger logger = LoggerFactory.getLogger(GenericStorageCacheManager.class);
    
    // Single storage for ALL entity types
    private GenericEntityStorage<CacheEntityTypeSet> storage;
    
    // WAL components
    private final WALProducer walProducer;
    private final WALConsumer walConsumer;
    
    // Database connection
    private final DataSource dataSource;
    
    // State management
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean replayComplete = new AtomicBoolean(false);
    
    // Statistics
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalEntriesProcessed = new AtomicLong(0);
    private final AtomicLong failedBatches = new AtomicLong(0);
    
    // Configuration
    private final int maxTotalRecords;
    private final Map<CacheEntityTypeSet, Integer> entityCapacities;
    
    /**
     * Create a new GenericStorageCacheManager with default configuration.
     */
    public GenericStorageCacheManager(WALProducer walProducer, WALConsumer walConsumer, 
                                     DataSource dataSource) {
        this(walProducer, walConsumer, dataSource, 1_000_000, null);
    }
    
    /**
     * Create a new GenericStorageCacheManager with custom configuration.
     */
    public GenericStorageCacheManager(WALProducer walProducer, WALConsumer walConsumer,
                                     DataSource dataSource, int maxTotalRecords,
                                     Map<CacheEntityTypeSet, Integer> entityCapacities) {
        this.walProducer = walProducer;
        this.walConsumer = walConsumer;
        this.dataSource = dataSource;
        this.maxTotalRecords = maxTotalRecords;
        this.entityCapacities = entityCapacities != null ? entityCapacities : createDefaultCapacities();
    }
    
    /**
     * Initialize the cache manager and storage.
     */
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            logger.info("Initializing GenericStorageCacheManager...");
            
            // Build the GenericEntityStorage
            var builder = GenericEntityStorage.<CacheEntityTypeSet>builder()
                .withEntityTypeSet(CacheEntityTypeSet.class)
                .withMaxRecords(maxTotalRecords);
            
            // Register entity types with their capacities
            if (entityCapacities != null && !entityCapacities.isEmpty()) {
                // Manual capacity distribution
                for (Map.Entry<CacheEntityTypeSet, Integer> entry : entityCapacities.entrySet()) {
                    Class<?> entityClass = getEntityClassForType(entry.getKey());
                    builder.registerType(entry.getKey(), entityClass, entry.getValue());
                    logger.info("Registered entity type: {} with capacity: {}", 
                        entry.getKey(), entry.getValue());
                }
            } else {
                // Auto-sizing - register all types and let storage distribute capacity
                builder.withAutoSizing();
                for (CacheEntityTypeSet type : CacheEntityTypeSet.values()) {
                    if (type != CacheEntityTypeSet.GENERIC_ENTITY) {
                        Class<?> entityClass = getEntityClassForType(type);
                        builder.registerType(type, entityClass);
                        logger.info("Registered entity type: {} (auto-sized)", type);
                    }
                }
            }
            
            storage = builder.build();
            
            // Replay WAL to restore cache state
            replayWAL();
            
            logger.info("GenericStorageCacheManager initialized successfully");
        }
    }
    
    /**
     * Process a batch of cache operations as a single atomic transaction.
     * All entities in the batch are written to the single GenericEntityStorage,
     * ensuring true atomicity across multiple entity types.
     */
    public BatchProcessResult processBatch(WALEntryBatch batch) {
        String transactionId = batch.getTransactionId();
        
        if (!initialized.get()) {
            return BatchProcessResult.failure(transactionId, "Cache manager not initialized");
        }
        
        logger.debug("Processing batch: transactionId={}, entries={}", transactionId, batch.size());
        
        Connection connection = null;
        List<ProcessedEntry> processedEntries = new ArrayList<>();
        
        try {
            // Step 1: Write entire batch to WAL (preserves transaction)
            walProducer.append(batch);
            logger.debug("Written batch to WAL: {}", transactionId);
            
            // Step 2: Get database connection and start transaction
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            
            // Step 3: Process each entry in the batch
            for (WALEntry entry : batch.getEntries()) {
                ProcessedEntry processed = processEntry(entry, connection);
                processedEntries.add(processed);
            }
            
            // Step 4: Commit database transaction
            connection.commit();
            
            // Step 5: Apply all entries to cache (after successful DB commit)
            for (ProcessedEntry processed : processedEntries) {
                applyToCache(processed);
            }
            
            // Update statistics
            totalBatchesProcessed.incrementAndGet();
            totalEntriesProcessed.addAndGet(processedEntries.size());
            
            logger.info("Successfully processed batch: transactionId={}, entries={}", 
                transactionId, processedEntries.size());
            return BatchProcessResult.success(transactionId, processedEntries.size());
            
        } catch (Exception e) {
            logger.error("Failed to process batch: {}", transactionId, e);
            failedBatches.incrementAndGet();
            
            // Rollback database transaction
            if (connection != null) {
                try {
                    connection.rollback();
                    logger.info("Rolled back transaction for batch: {}", transactionId);
                } catch (SQLException re) {
                    logger.error("Failed to rollback transaction", re);
                }
            }
            
            // Note: Cache is not modified on failure since we apply to cache after DB commit
            return BatchProcessResult.failure(transactionId, e.getMessage());
            
        } finally {
            // Close connection
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection", e);
                }
            }
        }
    }
    
    /**
     * Process a single WAL entry.
     */
    private ProcessedEntry processEntry(WALEntry entry, Connection connection) throws Exception {
        String tableName = entry.getTableName();
        String dbName = entry.getDbName();
        CacheEntityTypeSet entityType = CacheEntityTypeSet.fromTableName(tableName);
        
        // Convert WAL data to entity object
        Object entity = convertWALToEntity(entry, entityType);
        
        // Apply to database
        applyToDatabase(dbName, tableName, entry, connection);
        
        return new ProcessedEntry(entity, entityType, entry.getOperationType());
    }
    
    /**
     * Apply processed entry to cache.
     */
    private void applyToCache(ProcessedEntry processed) {
        switch (processed.operationType) {
            case INSERT:
            case UPDATE:
            case UPSERT:
                storage.put(processed.entity, processed.entityType);
                logger.trace("Applied {} to cache: type={}", 
                    processed.operationType, processed.entityType);
                break;
                
            case DELETE:
                Long id = extractId(processed.entity);
                if (id != null) {
                    storage.remove(id, processed.entityType);
                    logger.trace("Removed from cache: type={}, id={}", 
                        processed.entityType, id);
                }
                break;
                
            default:
                logger.warn("Unknown operation type: {}", processed.operationType);
        }
    }
    
    /**
     * Convert WAL entry data to entity object.
     * Creates a CacheEntity that GenericEntityStorage can work with.
     */
    private Object convertWALToEntity(WALEntry entry, CacheEntityTypeSet entityType) {
        Map<String, Object> data = new HashMap<>(entry.getData());
        
        // Ensure ID field exists
        if (!data.containsKey("id")) {
            // Try to find ID from common field names
            for (String field : Arrays.asList("_id", "ID", "Id", "pk", "key")) {
                if (data.containsKey(field)) {
                    data.put("id", data.get(field));
                    break;
                }
            }
        }
        
        // Create CacheEntity and populate fields
        CacheEntity entity = new CacheEntity();
        for (Map.Entry<String, Object> e : data.entrySet()) {
            entity.setField(e.getKey(), e.getValue());
        }
        
        return entity;
    }
    
    /**
     * Apply entry to database.
     */
    private void applyToDatabase(String dbName, String tableName, WALEntry entry, 
                                Connection connection) throws SQLException {
        switch (entry.getOperationType()) {
            case INSERT:
                executeInsert(tableName, entry.getData(), connection);
                break;
            case UPDATE:
                executeUpdate(tableName, entry.getData(), connection);
                break;
            case DELETE:
                executeDelete(tableName, entry.getData(), connection);
                break;
            case UPSERT:
                executeUpsert(tableName, entry.getData(), connection);
                break;
        }
    }
    
    /**
     * Execute INSERT operation.
     */
    private void executeInsert(String tableName, Map<String, Object> data, 
                              Connection connection) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        StringBuilder values = new StringBuilder("VALUES (");
        List<Object> params = new ArrayList<>();
        
        boolean first = true;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!first) {
                sql.append(", ");
                values.append(", ");
            }
            sql.append(entry.getKey());
            values.append("?");
            params.add(entry.getValue());
            first = false;
        }
        
        sql.append(") ").append(values).append(")");
        
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.executeUpdate();
        }
    }
    
    /**
     * Execute UPDATE operation.
     */
    private void executeUpdate(String tableName, Map<String, Object> data, 
                              Connection connection) throws SQLException {
        Object id = data.get("id");
        if (id == null) {
            throw new SQLException("UPDATE requires 'id' field");
        }
        
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        List<Object> params = new ArrayList<>();
        
        boolean first = true;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (!"id".equals(entry.getKey())) {
                if (!first) {
                    sql.append(", ");
                }
                sql.append(entry.getKey()).append(" = ?");
                params.add(entry.getValue());
                first = false;
            }
        }
        
        sql.append(" WHERE id = ?");
        params.add(id);
        
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            ps.executeUpdate();
        }
    }
    
    /**
     * Execute DELETE operation.
     */
    private void executeDelete(String tableName, Map<String, Object> data, 
                              Connection connection) throws SQLException {
        Object id = data.get("id");
        if (id == null) {
            throw new SQLException("DELETE requires 'id' field");
        }
        
        String sql = "DELETE FROM " + tableName + " WHERE id = ?";
        
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setObject(1, id);
            ps.executeUpdate();
        }
    }
    
    /**
     * Execute UPSERT operation.
     */
    private void executeUpsert(String tableName, Map<String, Object> data, 
                              Connection connection) throws SQLException {
        // Try update first
        try {
            executeUpdate(tableName, data, connection);
        } catch (SQLException e) {
            // If update fails (no rows affected), try insert
            executeInsert(tableName, data, connection);
        }
    }
    
    /**
     * Extract ID from entity object.
     */
    @SuppressWarnings("unchecked")
    private Long extractId(Object entity) {
        if (entity instanceof CacheEntity) {
            return ((CacheEntity) entity).getId();
        } else if (entity instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) entity;
            Object id = map.get("id");
            if (id instanceof Long) {
                return (Long) id;
            } else if (id instanceof Number) {
                return ((Number) id).longValue();
            }
        }
        return null;
    }
    
    /**
     * Replay WAL entries to restore cache state.
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
                    // Process each entry without database operations
                    for (WALEntry entry : batch.getEntries()) {
                        CacheEntityTypeSet entityType = 
                            CacheEntityTypeSet.fromTableName(entry.getTableName());
                        Object entity = convertWALToEntity(entry, entityType);
                        
                        ProcessedEntry processed = new ProcessedEntry(
                            entity, entityType, entry.getOperationType());
                        applyToCache(processed);
                        entryCount++;
                    }
                    batchCount++;
                }
            }
            
            logger.info("WAL replay complete: batches={}, entries={}", batchCount, entryCount);
            replayComplete.set(true);
            
        } catch (Exception e) {
            logger.error("Failed during WAL replay", e);
        }
    }
    
    /**
     * Get entity class for a given entity type.
     * Returns CacheEntity.class which can work with GenericEntityStorage.
     */
    private Class<?> getEntityClassForType(CacheEntityTypeSet entityType) {
        // All types use our CacheEntity wrapper class
        return CacheEntity.class;
    }
    
    /**
     * Create default capacities for entity types.
     */
    private Map<CacheEntityTypeSet, Integer> createDefaultCapacities() {
        Map<CacheEntityTypeSet, Integer> capacities = new HashMap<>();
        int perTypeCapacity = maxTotalRecords / CacheEntityTypeSet.values().length;
        
        for (CacheEntityTypeSet type : CacheEntityTypeSet.values()) {
            capacities.put(type, perTypeCapacity);
        }
        
        return capacities;
    }
    
    /**
     * Get a value from cache.
     */
    public Object get(String dbName, String tableName, Object key) {
        CacheEntityTypeSet entityType = CacheEntityTypeSet.fromTableName(tableName);
        Long id = key instanceof Long ? (Long) key : Long.valueOf(key.toString());
        return storage.get(id, entityType);
    }
    
    /**
     * Get all values for a specific entity type.
     */
    public List<?> getAll(String tableName) {
        CacheEntityTypeSet entityType = CacheEntityTypeSet.fromTableName(tableName);
        return storage.getAll(entityType);
    }
    
    /**
     * Check if a key exists.
     */
    public boolean contains(String tableName, Object key) {
        CacheEntityTypeSet entityType = CacheEntityTypeSet.fromTableName(tableName);
        Long id = key instanceof Long ? (Long) key : Long.valueOf(key.toString());
        return storage.exists(id, entityType);
    }
    
    /**
     * Get statistics.
     */
    public CacheStatistics getStatistics() {
        return new CacheStatistics(
            totalBatchesProcessed.get(),
            totalEntriesProcessed.get(),
            failedBatches.get(),
            storage.size(),
            replayComplete.get()
        );
    }
    
    /**
     * Shutdown the cache manager.
     */
    public void shutdown() {
        if (initialized.compareAndSet(true, false)) {
            logger.info("Shutting down GenericStorageCacheManager...");
            // Cleanup resources
            logger.info("GenericStorageCacheManager shutdown complete");
        }
    }
    
    // Inner classes
    
    /**
     * Represents a processed entry ready for cache application.
     */
    private static class ProcessedEntry {
        final Object entity;
        final CacheEntityTypeSet entityType;
        final WALEntry.OperationType operationType;
        
        ProcessedEntry(Object entity, CacheEntityTypeSet entityType, 
                      WALEntry.OperationType operationType) {
            this.entity = entity;
            this.entityType = entityType;
            this.operationType = operationType;
        }
    }
    
    /**
     * Result of batch processing.
     */
    public static class BatchProcessResult {
        private final boolean success;
        private final String transactionId;
        private final int entriesProcessed;
        private final String errorMessage;
        
        private BatchProcessResult(boolean success, String transactionId, 
                                  int entriesProcessed, String errorMessage) {
            this.success = success;
            this.transactionId = transactionId;
            this.entriesProcessed = entriesProcessed;
            this.errorMessage = errorMessage;
        }
        
        public static BatchProcessResult success(String transactionId, int entriesProcessed) {
            return new BatchProcessResult(true, transactionId, entriesProcessed, null);
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
     * Cache statistics.
     */
    public static class CacheStatistics {
        public final long totalBatchesProcessed;
        public final long totalEntriesProcessed;
        public final long failedBatches;
        public final int currentCacheSize;
        public final boolean replayComplete;
        
        CacheStatistics(long totalBatchesProcessed, long totalEntriesProcessed,
                       long failedBatches, int currentCacheSize, boolean replayComplete) {
            this.totalBatchesProcessed = totalBatchesProcessed;
            this.totalEntriesProcessed = totalEntriesProcessed;
            this.failedBatches = failedBatches;
            this.currentCacheSize = currentCacheSize;
            this.replayComplete = replayComplete;
        }
    }
}