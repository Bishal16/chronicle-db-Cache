package com.telcobright.api;

import com.telcobright.core.wal.WALEntry;
import java.util.List;

/**
 * Public API Proxy for Cache Service.
 * This is the ONLY public interface exposed to external clients.
 * All internal complexity is hidden behind these two simple methods.
 * 
 * This proxy delegates to the internal core implementation while
 * maintaining a clean, minimal public API surface.
 */
public interface CacheServiceProxy {
    
    /**
     * Process a batch of cache operations atomically.
     * This is the primary API for all multi-operation transactions.
     * Transaction ID will be automatically generated internally.
     * 
     * @param entries List of WAL entries to process as a single transaction
     * @return Result containing success status and operation details
     */
    CacheOperationResponse performCacheOpBatch(List<WALEntry> entries);
    
    /**
     * Process a single cache operation.
     * Internally creates a single-entry batch for consistency.
     * 
     * @param entry The WAL entry representing a single operation
     * @return Result containing success status and operation details
     */
    CacheOperationResponse performCacheOpSingle(WALEntry entry);
}