package com.telcobright.api;

import com.telcobright.core.wal.WALEntry;
import java.util.List;

/**
 * Public API Proxy for Cache Service.
 * This is the ONLY public interface exposed to external clients.
 * All internal complexity is hidden behind this simple method.
 * 
 * IMPORTANT: Always use List<WALEntry>, even for single entries.
 * This ensures consistent transaction handling and simplifies the API.
 * 
 * For single entries, simply use: List.of(entry)
 * 
 * This proxy delegates to the internal core implementation while
 * maintaining a clean, minimal public API surface.
 */
public interface CacheServiceProxy {
    
    /**
     * Process cache operations atomically.
     * 
     * This is the ONLY API method for all cache operations.
     * - For single operations: use List.of(entry)
     * - For batch operations: pass the list of entries
     * 
     * Transaction ID will be automatically generated internally.
     * All entries in the list will be processed as a single atomic transaction.
     * 
     * @param entries List of WAL entries (can contain 1 or more entries)
     * @return Result containing success status and operation details
     */
    CacheOperationResponse performCacheOpBatch(List<WALEntry> entries);
}