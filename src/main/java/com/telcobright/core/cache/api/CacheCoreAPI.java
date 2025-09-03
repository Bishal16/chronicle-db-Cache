package com.telcobright.core.cache.api;

import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Core cache API - the public interface for all cache operations.
 * This is the main entry point that adapters (gRPC, REST, embedded) will use.
 * 
 * The core is responsible for:
 * - WAL writing
 * - Cache updates
 * - Transaction management
 * - Consistency guarantees
 */
public interface CacheCoreAPI {
    
    /**
     * Process a batch of cache operations atomically.
     * This is the primary method for transactional operations.
     * 
     * @param batch The WAL entry batch containing multiple operations
     * @return Result of the batch operation
     */
    CacheOperationResult performCacheOpBatch(WALEntryBatch batch);
    
    /**
     * Process a single cache operation.
     * Internally creates a single-entry batch for consistency.
     * 
     * @param entry The WAL entry for a single operation
     * @return Result of the operation
     */
    CacheOperationResult performCacheOpSingle(WALEntry entry);
    
    /**
     * Process a batch asynchronously.
     * 
     * @param batch The WAL entry batch
     * @return CompletableFuture with the result
     */
    CompletableFuture<CacheOperationResult> performCacheOpBatchAsync(WALEntryBatch batch);
    
    /**
     * Process a single operation asynchronously.
     * 
     * @param entry The WAL entry
     * @return CompletableFuture with the result
     */
    CompletableFuture<CacheOperationResult> performCacheOpSingleAsync(WALEntry entry);
    
    /**
     * Process multiple batches in a single transaction.
     * Useful for complex multi-entity operations.
     * 
     * @param batches List of WAL entry batches
     * @return Combined result of all batches
     */
    CacheOperationResult performMultiBatch(List<WALEntryBatch> batches);
    
    /**
     * Query cache data (read-only operation).
     * 
     * @param query The cache query request
     * @return Query result
     */
    CacheQueryResult queryCache(CacheQuery query);
    
    /**
     * Get cache statistics and health information.
     * 
     * @return Current cache statistics
     */
    CacheStatistics getStatistics();
    
    /**
     * Check if the cache system is healthy and operational.
     * 
     * @return Health status
     */
    HealthStatus getHealthStatus();
    
    /**
     * Initialize the cache system.
     * Should be called once during application startup.
     */
    void initialize();
    
    /**
     * Shutdown the cache system gracefully.
     * Flushes pending operations and releases resources.
     */
    void shutdown();
    
    /**
     * Register a cache event listener.
     * 
     * @param listener The event listener
     */
    void registerListener(CacheEventListener listener);
    
    /**
     * Unregister a cache event listener.
     * 
     * @param listener The event listener to remove
     */
    void unregisterListener(CacheEventListener listener);
}