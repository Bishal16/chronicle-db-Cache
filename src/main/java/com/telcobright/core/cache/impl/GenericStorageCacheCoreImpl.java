package com.telcobright.core.cache.impl;

import com.telcobright.core.cache.api.*;
import com.telcobright.core.cache.GenericStorageCacheManager;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cache Core Implementation using GenericStorageCacheManager.
 * This implementation uses a single GenericEntityStorage for all entity types,
 * enabling true atomic batch operations across multiple entities.
 */
@ApplicationScoped
public class GenericStorageCacheCoreImpl implements CacheCoreAPI {
    private static final Logger logger = LoggerFactory.getLogger(GenericStorageCacheCoreImpl.class);
    
    @Inject
    private GenericStorageCacheManager cacheManager;
    
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private ExecutorService asyncExecutor;
    
    /**
     * Process a batch of cache operations.
     */
    @Override
    public CacheOperationResult performCacheOpBatch(WALEntryBatch batch) {
        if (!initialized.get()) {
            return CacheOperationResult.failure(batch.getTransactionId(), "Cache not initialized");
        }
        
        long startTime = System.currentTimeMillis();
        
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        long executionTime = System.currentTimeMillis() - startTime;
        
        if (result.isSuccess()) {
            return CacheOperationResult.builder()
                .transactionId(result.getTransactionId())
                .success(true)
                .operationsProcessed(result.getEntriesProcessed())
                .operationsFailed(0)
                .executionTimeMs(executionTime)
                .build();
        } else {
            return CacheOperationResult.builder()
                .transactionId(result.getTransactionId())
                .success(false)
                .operationsProcessed(0)
                .operationsFailed(batch.size())
                .errorMessage(result.getErrorMessage())
                .executionTimeMs(executionTime)
                .build();
        }
    }
    
    
    /**
     * Process multiple batches.
     */
    @Override
    public CacheOperationResult performMultiBatch(List<WALEntryBatch> batches) {
        if (!initialized.get()) {
            return CacheOperationResult.failure("MULTI_BATCH", "Cache not initialized");
        }
        
        int totalProcessed = 0;
        int failedBatches = 0;
        StringBuilder errorMessages = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
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
        
        long executionTime = System.currentTimeMillis() - startTime;
        
        if (failedBatches == 0) {
            return CacheOperationResult.builder()
                .transactionId("MULTI_BATCH")
                .success(true)
                .operationsProcessed(totalProcessed)
                .operationsFailed(0)
                .executionTimeMs(executionTime)
                .build();
        } else if (failedBatches == batches.size()) {
            return CacheOperationResult.builder()
                .transactionId("MULTI_BATCH")
                .success(false)
                .operationsProcessed(0)
                .operationsFailed(failedBatches)
                .errorMessage("All " + failedBatches + " batches failed: " + errorMessages.toString())
                .executionTimeMs(executionTime)
                .build();
        } else {
            // Partial success
            return CacheOperationResult.builder()
                .transactionId("MULTI_BATCH")
                .success(false)
                .operationsProcessed(totalProcessed)
                .operationsFailed(failedBatches)
                .errorMessage(failedBatches + " of " + batches.size() + " batches failed: " + errorMessages.toString())
                .executionTimeMs(executionTime)
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
     * Query cache for data.
     */
    @Override
    public CacheQueryResult queryCache(CacheQuery query) {
        if (!initialized.get()) {
            return CacheQueryResult.empty();
        }
        
        try {
            switch (query.getType()) {
                case GET_BY_KEY:
                    Object result = cacheManager.get(
                        query.getDatabase(),
                        query.getTable(),
                        query.getKey()
                    );
                    return result != null ? 
                        CacheQueryResult.single(result) : 
                        CacheQueryResult.empty();
                    
                case GET_ALL:
                    List<?> results = cacheManager.getAll(query.getTable());
                    return CacheQueryResult.multiple(new ArrayList<>(results));
                    
                case EXISTS:
                    boolean exists = cacheManager.contains(query.getTable(), query.getKey());
                    return CacheQueryResult.exists(exists);
                    
                default:
                    return CacheQueryResult.empty();
            }
        } catch (Exception e) {
            logger.error("Query failed: {}", query, e);
            return CacheQueryResult.error(e.getMessage());
        }
    }
    
    /**
     * Get cache statistics.
     */
    @Override
    public CacheStatistics getStatistics() {
        if (!initialized.get()) {
            return CacheStatistics.builder()
                .totalEntries(0)
                .cacheHits(0)
                .cacheMisses(0)
                .totalOperations(0)
                .successfulOperations(0)
                .failedOperations(0)
                .build();
        }
        
        GenericStorageCacheManager.CacheStatistics stats = cacheManager.getStatistics();
        
        return CacheStatistics.builder()
            .totalEntries(stats.currentCacheSize)
            .cacheHits(0) // hits not tracked in simplified version
            .cacheMisses(0) // misses not tracked
            .totalOperations(stats.totalEntriesProcessed)
            .successfulOperations(stats.totalEntriesProcessed - stats.failedBatches)
            .failedOperations(stats.failedBatches)
            .build();
    }
    
    /**
     * Get cache health status.
     */
    @Override
    public HealthStatus getHealthStatus() {
        if (!initialized.get()) {
            return HealthStatus.builder()
                .status(HealthStatus.Status.UNHEALTHY)
                .cacheHealthy(false)
                .addCheck(new HealthStatus.HealthCheck("cache", false, "Cache not initialized"))
                .build();
        }
        
        GenericStorageCacheManager.CacheStatistics stats = cacheManager.getStatistics();
        
        return HealthStatus.builder()
            .status(HealthStatus.Status.HEALTHY)
            .walHealthy(true)
            .cacheHealthy(true)
            .databaseHealthy(true)
            .addCheck(new HealthStatus.HealthCheck("cache", true, 
                String.format("Cache operational - Size: %d, Processed: %d batches/%d entries", 
                    stats.currentCacheSize, stats.totalBatchesProcessed, stats.totalEntriesProcessed)))
            .addCheck(new HealthStatus.HealthCheck("wal", true, 
                "WAL replay complete: " + stats.replayComplete))
            .build();
    }
    
    /**
     * Initialize the cache system.
     */
    @Override
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            logger.info("Initializing GenericStorage Cache Core...");
            
            // Create executor service for async operations
            asyncExecutor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r);
                t.setName("cache-async-" + t.getId());
                t.setDaemon(true);
                return t;
            });
            
            // Initialize the injected cache manager
            if (cacheManager != null) {
                cacheManager.initialize();
                logger.info("GenericStorage Cache Core initialized successfully");
            } else {
                logger.error("GenericStorageCacheManager not injected!");
                initialized.set(false);
            }
        }
    }
    
    /**
     * Shutdown the cache system.
     */
    @Override
    public void shutdown() {
        if (initialized.compareAndSet(true, false)) {
            logger.info("Shutting down GenericStorage Cache Core...");
            
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
            
            logger.info("GenericStorage Cache Core shutdown complete");
        }
    }
    
    /**
     * Register an event listener.
     */
    @Override
    public void registerListener(CacheEventListener listener) {
        // Event listeners can be implemented if needed
        logger.info("Event listener registration not implemented in this version");
    }
    
    /**
     * Unregister an event listener.
     */
    @Override
    public void unregisterListener(CacheEventListener listener) {
        // Event listeners can be implemented if needed
        logger.info("Event listener unregistration not implemented in this version");
    }
}