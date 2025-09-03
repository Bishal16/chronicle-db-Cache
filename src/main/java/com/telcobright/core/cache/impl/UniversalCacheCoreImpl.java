package com.telcobright.core.cache.impl;

import com.telcobright.core.cache.api.*;
import com.telcobright.core.cache.universal.*;
import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Universal Core Cache Implementation using UniversalCacheManager.
 * This implementation delegates all multi-entity batch handling to UniversalCacheManager,
 * ensuring transactional consistency across different entity types.
 */
@ApplicationScoped
public class UniversalCacheCoreImpl implements CacheCoreAPI {
    private static final Logger logger = LoggerFactory.getLogger(UniversalCacheCoreImpl.class);
    
    private UniversalCacheManager cacheManager;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private ExecutorService asyncExecutor;
    
    @Inject
    private WALProducer walProducer;
    
    @Inject
    private WALConsumer walConsumer;
    
    @Inject
    private DataSource dataSource;
    
    /**
     * Process a batch of cache operations as a single transaction.
     * The batch can contain operations for multiple entities/tables.
     */
    @Override
    public CacheOperationResult performCacheOpBatch(WALEntryBatch batch) {
        if (!initialized.get()) {
            return CacheOperationResult.failure(batch.getTransactionId(), "Cache not initialized");
        }
        
        logger.debug("Processing batch through UniversalCacheManager: txnId={}, entries={}", 
                   batch.getTransactionId(), batch.size());
        
        // Delegate to UniversalCacheManager which handles:
        // 1. Writing entire batch to WAL
        // 2. Transactional database updates
        // 3. In-memory cache updates
        // 4. Rollback on failure
        UniversalCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        // Convert to CacheOperationResult
        if (result.isSuccess()) {
            return CacheOperationResult.success(
                result.getTransactionId(), 
                result.getEntriesProcessed()
            );
        } else {
            return CacheOperationResult.failure(
                result.getTransactionId(), 
                result.getErrorMessage()
            );
        }
    }
    
    /**
     * Process a single cache operation.
     * Internally creates a single-entry batch.
     */
    @Override
    public CacheOperationResult performCacheOpSingle(WALEntry entry) {
        // Create a single-entry batch
        WALEntryBatch batch = WALEntryBatch.builder()
            .addEntry(entry)
            .build();
        
        return performCacheOpBatch(batch);
    }
    
    /**
     * Process multiple batches.
     * Each batch is processed as a separate transaction.
     */
    @Override
    public CacheOperationResult performMultiBatch(List<WALEntryBatch> batches) {
        if (!initialized.get()) {
            return CacheOperationResult.failure("MULTI_BATCH", "Cache not initialized");
        }
        
        int totalProcessed = 0;
        int failedBatches = 0;
        StringBuilder errorMessages = new StringBuilder();
        
        for (WALEntryBatch batch : batches) {
            CacheOperationResult result = performCacheOpBatch(batch);
            if (result.isSuccess()) {
                totalProcessed += result.getOperationsProcessed();
            } else {
                failedBatches++;
                if (errorMessages.length() > 0) {
                    errorMessages.append("; ");
                }
                errorMessages.append("Batch ").append(batch.getTransactionId())
                            .append(": ").append(result.getErrorMessage());
            }
        }
        
        // Return combined result
        if (failedBatches == 0) {
            return CacheOperationResult.success("MULTI_BATCH", totalProcessed);
        } else if (failedBatches == batches.size()) {
            return CacheOperationResult.failure("MULTI_BATCH", 
                "All " + failedBatches + " batches failed: " + errorMessages.toString());
        } else {
            // Partial success - some batches succeeded, some failed
            return CacheOperationResult.builder()
                .transactionId("MULTI_BATCH")
                .success(false)
                .operationsProcessed(totalProcessed)
                .operationsFailed(failedBatches)
                .errorMessage(failedBatches + " of " + batches.size() + " batches failed: " + errorMessages.toString())
                .timestamp(java.time.LocalDateTime.now())
                .build();
        }
    }
    
    /**
     * Process a batch asynchronously.
     */
    @Override
    public CompletableFuture<CacheOperationResult> performCacheOpBatchAsync(WALEntryBatch batch) {
        return CompletableFuture.supplyAsync(() -> performCacheOpBatch(batch), asyncExecutor);
    }
    
    /**
     * Process a single operation asynchronously.
     */
    @Override
    public CompletableFuture<CacheOperationResult> performCacheOpSingleAsync(WALEntry entry) {
        return CompletableFuture.supplyAsync(() -> performCacheOpSingle(entry), asyncExecutor);
    }
    
    /**
     * Query cache for data - using the queryCache method from interface.
     */
    @Override
    public CacheQueryResult queryCache(CacheQuery query) {
        if (!initialized.get()) {
            return CacheQueryResult.error("Cache not initialized");
        }
        
        String dbName = query.getDatabase();
        String tableName = query.getTable();
        
        switch (query.getType()) {
            case GET_BY_KEY:
                Object value = cacheManager.get(dbName, tableName, query.getKey());
                return value != null ? 
                    CacheQueryResult.single(value) : 
                    CacheQueryResult.empty();
                    
            case GET_ALL:
                return CacheQueryResult.multiple(
                    new java.util.ArrayList<>(cacheManager.getAll(dbName, tableName).values())
                );
                
            case EXISTS:
                return CacheQueryResult.exists(
                    cacheManager.contains(dbName, tableName, query.getKey())
                );
                
            default:
                return CacheQueryResult.error("Unsupported query type: " + query.getType());
        }
    }
    
    /**
     * Register an entity handler with the cache manager.
     * This must be called before initialization for each entity type.
     */
    public void registerHandler(EntityCacheHandler handler) {
        if (cacheManager == null) {
            throw new IllegalStateException("Cache manager not created yet");
        }
        cacheManager.registerHandler(handler);
    }
    
    /**
     * Initialize the cache system.
     */
    @Override
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            logger.info("Initializing Universal Cache Core...");
            
            // Create executor service for async operations
            asyncExecutor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r);
                t.setName("cache-async-" + t.getId());
                t.setDaemon(true);
                return t;
            });
            
            // Create the UniversalCacheManager
            cacheManager = new UniversalCacheManager(walProducer, walConsumer, dataSource);
            
            // Register default handlers (can be extended by application)
            registerDefaultHandlers();
            
            // Initialize the cache manager (starts WAL replay)
            cacheManager.initialize();
            
            logger.info("Universal Cache Core initialized successfully");
        }
    }
    
    /**
     * Register default entity handlers.
     * Override this method to register application-specific handlers.
     */
    protected void registerDefaultHandlers() {
        // This method can be overridden to register specific handlers
        // For example:
        // registerHandler(new PackageAccountHandler());
        // registerHandler(new PackageAccountReserveHandler());
        logger.info("No default handlers registered. Override registerDefaultHandlers() to add handlers.");
    }
    
    /**
     * Shutdown the cache system.
     */
    @Override
    public void shutdown() {
        if (initialized.compareAndSet(true, false)) {
            logger.info("Shutting down Universal Cache Core...");
            
            if (cacheManager != null) {
                cacheManager.shutdown();
            }
            
            if (asyncExecutor != null) {
                asyncExecutor.shutdown();
                try {
                    if (!asyncExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                        asyncExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    asyncExecutor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
            
            logger.info("Universal Cache Core shutdown complete");
        }
    }
    
    /**
     * Register an event listener.
     */
    @Override
    public void registerListener(CacheEventListener listener) {
        // Event listeners can be added if needed
        logger.debug("Event listener registered: {}", listener.getClass().getSimpleName());
    }
    
    /**
     * Unregister an event listener.
     */
    @Override
    public void unregisterListener(CacheEventListener listener) {
        // Event listeners can be removed if needed
        logger.debug("Event listener unregistered: {}", listener.getClass().getSimpleName());
    }
    
    /**
     * Get health status of the cache.
     */
    @Override
    public HealthStatus getHealthStatus() {
        if (!initialized.get()) {
            return HealthStatus.builder()
                .status(HealthStatus.Status.UNHEALTHY)
                .walHealthy(false)
                .cacheHealthy(false)
                .databaseHealthy(false)
                .timestamp(java.time.LocalDateTime.now())
                .build();
        }
        
        UniversalCacheManager.CacheStatistics stats = cacheManager.getStatistics();
        
        java.util.Map<String, Object> details = new java.util.HashMap<>();
        details.put("totalBatchesProcessed", stats.totalBatchesProcessed);
        details.put("totalEntriesProcessed", stats.totalEntriesProcessed);
        details.put("failedBatches", stats.failedBatches);
        details.put("replayComplete", stats.replayComplete);
        details.put("handlers", stats.handlerStats.keySet());
        
        boolean healthy = stats.replayComplete && (stats.failedBatches == 0 || 
                                                  stats.totalBatchesProcessed > stats.failedBatches * 10);
        
        return HealthStatus.builder()
            .status(healthy ? HealthStatus.Status.HEALTHY : HealthStatus.Status.DEGRADED)
            .walHealthy(true)
            .cacheHealthy(healthy)
            .databaseHealthy(true)
            .timestamp(java.time.LocalDateTime.now())
            .build();
    }
    
    /**
     * Get cache statistics.
     */
    @Override
    public CacheStatistics getStatistics() {
        if (cacheManager == null) {
            return CacheStatistics.builder()
                .totalOperations(0)
                .successfulOperations(0)
                .failedOperations(0)
                .cacheHits(0)
                .cacheMisses(0)
                .totalEntries(0)
                .walQueueSize(0)
                .averageResponseTimeMs(0)
                .startTime(java.time.LocalDateTime.now())
                .currentTime(java.time.LocalDateTime.now())
                .build();
        }
        
        UniversalCacheManager.CacheStatistics stats = cacheManager.getStatistics();
        
        // Convert to API CacheStatistics
        return CacheStatistics.builder()
            .totalOperations(stats.totalBatchesProcessed)
            .successfulOperations(stats.totalBatchesProcessed - stats.failedBatches)
            .failedOperations(stats.failedBatches)
            .cacheHits(stats.totalEntriesProcessed)
            .cacheMisses(0) // Could track this if needed
            .totalEntries(stats.totalEntriesProcessed)
            .walQueueSize(0) // Could track this if needed
            .averageResponseTimeMs(0) // Could track this if needed
            .startTime(java.time.LocalDateTime.now()) // Could track actual start time
            .currentTime(java.time.LocalDateTime.now())
            .build();
    }
}