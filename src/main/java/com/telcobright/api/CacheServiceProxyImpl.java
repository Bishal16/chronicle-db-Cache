package com.telcobright.api;

import com.telcobright.core.cache.api.CacheCoreAPI;
import com.telcobright.core.cache.api.CacheOperationResult;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;

/**
 * Implementation of the public Cache Service Proxy.
 * This class acts as a facade/proxy that:
 * 1. Exposes ONLY the two public APIs
 * 2. Delegates to internal core implementation
 * 3. Converts between public API types and internal types
 * 4. Hides all internal complexity from external clients
 * 
 * This is the ONLY entry point for gRPC, REST, and other adapters.
 */
@ApplicationScoped
public class CacheServiceProxyImpl implements CacheServiceProxy {
    private static final Logger logger = LoggerFactory.getLogger(CacheServiceProxyImpl.class);
    
    @Inject
    private CacheCoreAPI cacheCore;
    
    /**
     * Process a batch of cache operations.
     * Transaction ID is generated internally - clients don't need to provide it.
     * Delegates to internal core and converts response.
     */
    @Override
    public CacheOperationResponse performCacheOpBatch(List<WALEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return CacheOperationResponse.failure("INVALID_BATCH", "Entries cannot be null or empty");
        }
        
        // Create batch with auto-generated transaction ID
        String transactionId = "TXN_" + System.currentTimeMillis() + "_" + UUID.randomUUID();
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId(transactionId)
            .entries(entries)
            .build();
        
        logger.debug("Processing batch: transactionId={}, size={}", 
                   batch.getTransactionId(), batch.size());
        
        try {
            // Delegate to internal core
            CacheOperationResult internalResult = cacheCore.performCacheOpBatch(batch);
            
            // Convert internal result to public API response
            return convertToPublicResponse(internalResult);
            
        } catch (Exception e) {
            logger.error("Error processing batch: {}", batch.getTransactionId(), e);
            return CacheOperationResponse.failure(batch.getTransactionId(), 
                                                 "Internal error: " + e.getMessage());
        }
    }
    
    /**
     * Process a single cache operation.
     * Creates a single-entry batch and delegates to batch API.
     */
    @Override
    public CacheOperationResponse performCacheOpSingle(WALEntry entry) {
        if (entry == null) {
            return CacheOperationResponse.failure("INVALID_ENTRY", "Entry cannot be null");
        }
        
        logger.debug("Processing single entry as batch: table={}", 
                   entry.getTableName());
        
        // Delegate to batch processing using the new API
        return performCacheOpBatch(List.of(entry));
    }
    
    /**
     * Convert internal result to public API response.
     * This keeps the public API clean and simple.
     */
    private CacheOperationResponse convertToPublicResponse(CacheOperationResult internalResult) {
        if (internalResult.isSuccess()) {
            return CacheOperationResponse.success(
                internalResult.getTransactionId(),
                internalResult.getOperationsProcessed()
            );
        } else {
            return CacheOperationResponse.failure(
                internalResult.getTransactionId(),
                internalResult.getErrorMessage()
            );
        }
    }
    
    /**
     * Initialize the cache service.
     * Called during application startup.
     */
    public void initialize() {
        logger.info("Initializing Cache Service Proxy...");
        
        if (cacheCore == null) {
            throw new IllegalStateException("CacheCore not injected");
        }
        
        cacheCore.initialize();
        logger.info("Cache Service Proxy initialized successfully");
    }
    
    /**
     * Shutdown the cache service.
     * Called during application shutdown.
     */
    public void shutdown() {
        logger.info("Shutting down Cache Service Proxy...");
        
        if (cacheCore != null) {
            cacheCore.shutdown();
        }
        
        logger.info("Cache Service Proxy shutdown complete");
    }
    
    /**
     * Health check for monitoring.
     * Returns true if service is operational.
     */
    public boolean isHealthy() {
        if (cacheCore == null) {
            return false;
        }
        
        return cacheCore.getHealthStatus().isHealthy();
    }
}