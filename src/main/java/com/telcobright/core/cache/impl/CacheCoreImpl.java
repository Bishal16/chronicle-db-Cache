package com.telcobright.core.cache.impl;

import com.telcobright.core.cache.api.*;
import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.cache.wal.processor.BatchProcessor;
import com.telcobright.core.cache.EntityCache;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core cache implementation.
 * This is the central component that coordinates all cache operations.
 * 
 * Architecture:
 * - Receives operations from adapters (gRPC, REST, embedded)
 * - Writes to WAL for durability
 * - Updates in-memory caches
 * - Manages transactions and consistency
 * - Handles recovery and replay
 */
@ApplicationScoped
public class CacheCoreImpl implements CacheCoreAPI {
    private static final Logger logger = LoggerFactory.getLogger(CacheCoreImpl.class);
    
    @Inject
    private WALProducer walProducer;
    
    @Inject
    private WALConsumer walConsumer;
    
    @Inject
    private Connection dbConnection;
    
    // Cache registry - maps table name to EntityCache instance
    private final Map<String, EntityCache<?, ?>> cacheRegistry = new ConcurrentHashMap<>();
    
    // Event listeners
    private final List<CacheEventListener> listeners = new CopyOnWriteArrayList<>();
    
    // Statistics
    private final AtomicLong totalOperations = new AtomicLong();
    private final AtomicLong successfulOperations = new AtomicLong();
    private final AtomicLong failedOperations = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    
    // Executor for async operations
    private final ExecutorService asyncExecutor = Executors.newFixedThreadPool(10);
    
    // Batch processor for replay
    private BatchProcessor batchProcessor;
    
    // System state
    private volatile boolean initialized = false;
    private volatile boolean shuttingDown = false;
    private LocalDateTime startTime;
    
    @Override
    public void initialize() {
        if (initialized) {
            logger.warn("Cache core already initialized");
            return;
        }
        
        logger.info("Initializing cache core...");
        startTime = LocalDateTime.now();
        
        try {
            // Initialize batch processor for WAL replay
            batchProcessor = new BatchProcessor(
                walConsumer,
                dbConnection,
                this::applyWALEntryToCache
            );
            
            // Replay pending WAL entries
            replayWAL();
            
            initialized = true;
            notifyListeners(new CacheEventListener.CacheEvent(
                null, CacheEventListener.CacheEvent.EventType.REPLAY_COMPLETE, null));
            
            // Notify initialization complete
            listeners.forEach(listener -> listener.onCacheInitialized());
            
            logger.info("Cache core initialized successfully");
            
        } catch (Exception e) {
            logger.error("Failed to initialize cache core", e);
            throw new RuntimeException("Cache initialization failed", e);
        }
    }
    
    @Override
    public CacheOperationResult performCacheOpBatch(WALEntryBatch batch) {
        if (!initialized) {
            return CacheOperationResult.failure(batch.getTransactionId(), 
                "Cache not initialized");
        }
        
        long startTime = System.currentTimeMillis();
        String transactionId = batch.getTransactionId();
        
        if (transactionId == null) {
            transactionId = "TXN_" + UUID.randomUUID();
            batch.setTransactionId(transactionId);
        }
        
        totalOperations.incrementAndGet();
        
        // Notify listeners before operation
        notifyListeners(new CacheEventListener.CacheEvent(
            transactionId, CacheEventListener.CacheEvent.EventType.BATCH_START, batch));
        
        try {
            // 1. Write to WAL first (durability)
            long walOffset = walProducer.append(batch);
            logger.debug("Wrote batch {} to WAL at offset {}", transactionId, walOffset);
            
            // 2. Apply to cache (in-memory)
            int processed = applyBatchToCache(batch);
            
            // 3. Success - update statistics
            successfulOperations.incrementAndGet();
            
            CacheOperationResult result = CacheOperationResult.builder()
                .transactionId(transactionId)
                .success(true)
                .operationsProcessed(processed)
                .executionTimeMs(System.currentTimeMillis() - startTime)
                .build();
            
            // Notify listeners after operation
            notifyListeners(new CacheEventListener.CacheEvent(
                transactionId, CacheEventListener.CacheEvent.EventType.BATCH_COMPLETE, result));
            
            return result;
            
        } catch (Exception e) {
            logger.error("Failed to process batch {}", transactionId, e);
            failedOperations.incrementAndGet();
            
            // Notify error
            notifyListeners(new CacheEventListener.CacheEvent(
                transactionId, CacheEventListener.CacheEvent.EventType.BATCH_ERROR, batch, e));
            
            return CacheOperationResult.failure(transactionId, e.getMessage());
        }
    }
    
    @Override
    public CacheOperationResult performCacheOpSingle(WALEntry entry) {
        // Create single-entry batch
        String transactionId = "TXN_" + UUID.randomUUID();
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId(transactionId)
            .addEntry(entry)
            .build();
        
        return performCacheOpBatch(batch);
    }
    
    @Override
    public CompletableFuture<CacheOperationResult> performCacheOpBatchAsync(WALEntryBatch batch) {
        return CompletableFuture.supplyAsync(
            () -> performCacheOpBatch(batch),
            asyncExecutor
        );
    }
    
    @Override
    public CompletableFuture<CacheOperationResult> performCacheOpSingleAsync(WALEntry entry) {
        return CompletableFuture.supplyAsync(
            () -> performCacheOpSingle(entry),
            asyncExecutor
        );
    }
    
    @Override
    public CacheOperationResult performMultiBatch(List<WALEntryBatch> batches) {
        if (batches == null || batches.isEmpty()) {
            return CacheOperationResult.success("MULTI_" + UUID.randomUUID(), 0);
        }
        
        String multiTransactionId = "MULTI_" + UUID.randomUUID();
        long startTime = System.currentTimeMillis();
        
        int totalProcessed = 0;
        int totalFailed = 0;
        List<CacheOperationResult.EntityResult> allResults = new ArrayList<>();
        
        for (WALEntryBatch batch : batches) {
            CacheOperationResult result = performCacheOpBatch(batch);
            
            if (result.isSuccess()) {
                totalProcessed += result.getOperationsProcessed();
            } else {
                totalFailed += result.getOperationsFailed();
            }
            
            allResults.addAll(result.getEntityResults());
        }
        
        return CacheOperationResult.builder()
            .transactionId(multiTransactionId)
            .success(totalFailed == 0)
            .operationsProcessed(totalProcessed)
            .operationsFailed(totalFailed)
            .entityResults(allResults)
            .executionTimeMs(System.currentTimeMillis() - startTime)
            .build();
    }
    
    @Override
    public CacheQueryResult queryCache(CacheQuery query) {
        long startTime = System.currentTimeMillis();
        
        try {
            EntityCache<?, ?> cache = cacheRegistry.get(query.getTable());
            if (cache == null) {
                return CacheQueryResult.error("Cache not found for table: " + query.getTable());
            }
            
            switch (query.getType()) {
                case GET_BY_KEY:
                    // Use raw types to handle wildcards
                    @SuppressWarnings("unchecked")
                    EntityCache rawCache = (EntityCache) cache;
                    Object value = rawCache.get(query.getDatabase(), query.getKey());
                    if (value != null) {
                        cacheHits.incrementAndGet();
                        return CacheQueryResult.single(value);
                    } else {
                        cacheMisses.incrementAndGet();
                        return CacheQueryResult.empty();
                    }
                    
                case GET_ALL:
                    Map<?, ?> all = cache.getAll(query.getDatabase());
                    List<Object> values = new ArrayList<>(all.values());
                    return CacheQueryResult.multiple(values);
                    
                case COUNT:
                    int count = cache.size(query.getDatabase());
                    return CacheQueryResult.count(count);
                    
                case EXISTS:
                    // Use raw types to handle wildcards
                    @SuppressWarnings("unchecked")
                    EntityCache rawCache2 = (EntityCache) cache;
                    boolean exists = rawCache2.contains(query.getDatabase(), query.getKey());
                    return CacheQueryResult.exists(exists);
                    
                default:
                    return CacheQueryResult.error("Unsupported query type: " + query.getType());
            }
            
        } catch (Exception e) {
            logger.error("Query failed", e);
            return CacheQueryResult.error(e.getMessage());
        }
    }
    
    @Override
    public CacheStatistics getStatistics() {
        return CacheStatistics.builder()
            .totalOperations(totalOperations.get())
            .successfulOperations(successfulOperations.get())
            .failedOperations(failedOperations.get())
            .cacheHits(cacheHits.get())
            .cacheMisses(cacheMisses.get())
            .totalEntries(getTotalCacheEntries())
            .walQueueSize(0) // TODO: Get from WAL producer
            .averageResponseTimeMs(0) // TODO: Calculate average
            .startTime(startTime)
            .currentTime(LocalDateTime.now())
            .build();
    }
    
    @Override
    public HealthStatus getHealthStatus() {
        boolean walHealthy = walProducer != null && walProducer.isHealthy();
        boolean cacheHealthy = !cacheRegistry.isEmpty() && initialized;
        boolean dbHealthy = checkDatabaseHealth();
        
        HealthStatus.Builder builder = HealthStatus.builder()
            .walHealthy(walHealthy)
            .cacheHealthy(cacheHealthy)
            .databaseHealthy(dbHealthy);
        
        if (walHealthy && cacheHealthy && dbHealthy) {
            builder.status(HealthStatus.Status.HEALTHY);
        } else if (walHealthy || cacheHealthy) {
            builder.status(HealthStatus.Status.DEGRADED);
        } else {
            builder.status(HealthStatus.Status.UNHEALTHY);
        }
        
        return builder.build();
    }
    
    @Override
    public void shutdown() {
        if (shuttingDown) {
            return;
        }
        
        shuttingDown = true;
        logger.info("Shutting down cache core...");
        
        // Notify listeners
        listeners.forEach(listener -> listener.onCacheShutdown());
        
        try {
            // Flush WAL
            if (walProducer != null) {
                walProducer.flush();
                walProducer.close();
            }
            
            // Close consumer
            if (walConsumer != null) {
                walConsumer.close();
            }
            
            // Shutdown executor
            asyncExecutor.shutdown();
            asyncExecutor.awaitTermination(10, TimeUnit.SECONDS);
            
            // Clear caches
            cacheRegistry.clear();
            
            logger.info("Cache core shutdown complete");
            
        } catch (Exception e) {
            logger.error("Error during shutdown", e);
        }
    }
    
    @Override
    public void registerListener(CacheEventListener listener) {
        listeners.add(listener);
    }
    
    @Override
    public void unregisterListener(CacheEventListener listener) {
        listeners.remove(listener);
    }
    
    /**
     * Register an entity cache.
     */
    public void registerCache(String tableName, EntityCache<?, ?> cache) {
        cacheRegistry.put(tableName, cache);
        logger.info("Registered cache for table: {}", tableName);
    }
    
    // Private helper methods
    
    private void replayWAL() throws Exception {
        logger.info("Starting WAL replay...");
        
        int replayCount = 0;
        int batchCount;
        
        do {
            batchCount = batchProcessor.processNextBatch();
            replayCount += batchCount;
        } while (batchCount > 0);
        
        logger.info("WAL replay complete. Processed {} entries", replayCount);
    }
    
    private int applyBatchToCache(WALEntryBatch batch) {
        int processed = 0;
        
        for (WALEntry entry : batch.getEntries()) {
            try {
                applyWALEntryToCache(entry, dbConnection);
                processed++;
            } catch (Exception e) {
                logger.error("Failed to apply entry to cache", e);
            }
        }
        
        return processed;
    }
    
    @SuppressWarnings("unchecked")
    private void applyWALEntryToCache(WALEntry entry, Connection conn) {
        String tableName = entry.getTableName();
        EntityCache cache = cacheRegistry.get(tableName);
        
        if (cache == null) {
            logger.warn("No cache registered for table: {}", tableName);
            return;
        }
        
        String dbName = entry.getDbName();
        Map<String, Object> data = entry.getData();
        
        // Extract primary key (assuming "id" field)
        Object key = data.get("id");
        if (key == null) {
            logger.warn("No primary key in WAL entry for table: {}", tableName);
            return;
        }
        
        switch (entry.getOperationType()) {
            case INSERT:
            case UPDATE:
            case UPSERT:
                // For now, just put the data map as the entity
                // In real implementation, would deserialize to proper entity type
                // Use raw type to handle wildcard
                @SuppressWarnings("unchecked")
                EntityCache rawCache = (EntityCache) cache;
                rawCache.put(dbName, data);
                break;
                
            case DELETE:
                cache.delete(dbName, key);
                break;
                
            default:
                logger.warn("Unknown operation type: {}", entry.getOperationType());
        }
    }
    
    private boolean checkDatabaseHealth() {
        try {
            return dbConnection != null && !dbConnection.isClosed();
        } catch (Exception e) {
            return false;
        }
    }
    
    private long getTotalCacheEntries() {
        return cacheRegistry.values().stream()
            .mapToInt(cache -> cache.totalSize())
            .sum();
    }
    
    private void notifyListeners(CacheEventListener.CacheEvent event) {
        for (CacheEventListener listener : listeners) {
            try {
                switch (event.getType()) {
                    case BATCH_START:
                        listener.onBeforeOperation(event);
                        break;
                    case BATCH_COMPLETE:
                        listener.onAfterOperation(event);
                        break;
                    case BATCH_ERROR:
                        listener.onError(event);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error notifying listener", e);
            }
        }
    }
}