package com.telcobright.core.cache.api;

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
     * This is the PRIMARY and ONLY method for cache operations.
     * 
     * IMPORTANT: Always use WALEntryBatch, even for single entries.
     * This ensures consistent transaction handling and atomicity.
     * 
     * For single entries, use: 
     *   WALEntryBatch.builder()
     *     .transactionId("TXN_" + System.currentTimeMillis())
     *     .addEntry(entry)
     *     .build()
     * 
     * @param batch The WAL entry batch containing one or more operations
     * @return Result of the batch operation
     */
    CacheOperationResult performCacheOpBatch(WALEntryBatch batch);
    
    /**
     * Process a batch asynchronously.
     * 
     * @param batch The WAL entry batch (can contain single or multiple entries)
     * @return CompletableFuture with the result
     */
    CompletableFuture<CacheOperationResult> performCacheOpBatchAsync(WALEntryBatch batch);
    
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