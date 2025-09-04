package com.telcobright.core.adapter.rest;

import com.telcobright.api.CacheServiceProxy;
import com.telcobright.api.CacheServiceProxyImpl;
import com.telcobright.api.CacheOperationResponse;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;

/**
 * REST Adapter for Cache Service.
 * 
 * This adapter:
 * 1. Receives HTTP/REST requests
 * 2. Converts JSON to domain objects (WALEntry/WALEntryBatch)
 * 3. Calls the CacheServiceProxy (the ONLY public API)
 * 4. Returns JSON response
 * 
 * Like the gRPC adapter, this knows NOTHING about internal implementation,
 * only the two public APIs in CacheServiceProxy.
 */
@Path("/api/cache")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class RestCacheAdapter {
    private static final Logger logger = LoggerFactory.getLogger(RestCacheAdapter.class);
    
    @Inject
    private CacheServiceProxy cacheService;
    
    /**
     * Process a batch of cache operations.
     * Transaction ID is generated internally by the library.
     * 
     * POST /api/cache/batch
     * Body: List of WALEntry objects in JSON format
     */
    @POST
    @Path("/batch")
    public Response processBatch(List<WALEntry> entries) {
        logger.debug("Received REST batch request: size={}", 
                    entries != null ? entries.size() : 0);
        
        try {
            // Call the public API (proxy) - transaction ID generated internally
            CacheOperationResponse response = cacheService.performCacheOpBatch(entries);
            
            // Return JSON response
            if (response.isSuccess()) {
                logger.debug("REST batch processed successfully: processed={}", 
                           response.getOperationsProcessed());
                return Response.ok(response).build();
            } else {
                logger.warn("REST batch failed: {}", response.getErrorMessage());
                return Response.status(Response.Status.BAD_REQUEST)
                              .entity(response)
                              .build();
            }
            
        } catch (Exception e) {
            logger.error("Error processing REST batch request", e);
            
            CacheOperationResponse errorResponse = CacheOperationResponse.failure(
                "BATCH_ERROR", 
                "Internal server error: " + e.getMessage()
            );
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                          .entity(errorResponse)
                          .build();
        }
    }
    
    /**
     * Process a single cache operation.
     * 
     * POST /api/cache/single
     * Body: WALEntry in JSON format
     */
    @POST
    @Path("/single")
    public Response processSingle(WALEntry entry) {
        logger.debug("Received REST single operation: table={}, op={}", 
                    entry.getTableName(), entry.getOperationType());
        
        try {
            // Call the public API (proxy) - always use List even for single entry
            CacheOperationResponse response = cacheService.performCacheOpBatch(List.of(entry));
            
            // Return JSON response
            if (response.isSuccess()) {
                logger.debug("REST single operation processed successfully");
                return Response.ok(response).build();
            } else {
                logger.warn("REST single operation failed: {}", response.getErrorMessage());
                return Response.status(Response.Status.BAD_REQUEST)
                              .entity(response)
                              .build();
            }
            
        } catch (Exception e) {
            logger.error("Error processing REST single operation", e);
            
            CacheOperationResponse errorResponse = CacheOperationResponse.failure(
                "SINGLE_OP_ERROR", 
                "Internal server error: " + e.getMessage()
            );
            
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                          .entity(errorResponse)
                          .build();
        }
    }
    
    /**
     * Health check endpoint.
     * 
     * GET /api/cache/health
     */
    @GET
    @Path("/health")
    public Response health() {
        boolean healthy = ((CacheServiceProxyImpl) cacheService).isHealthy();
        
        HealthResponse healthResponse = new HealthResponse(
            healthy ? "HEALTHY" : "UNHEALTHY",
            healthy
        );
        
        if (healthy) {
            return Response.ok(healthResponse).build();
        } else {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                          .entity(healthResponse)
                          .build();
        }
    }
    
    /**
     * Simple health response object.
     */
    public static class HealthResponse {
        private final String status;
        private final boolean healthy;
        
        public HealthResponse(String status, boolean healthy) {
            this.status = status;
            this.healthy = healthy;
        }
        
        public String getStatus() { return status; }
        public boolean isHealthy() { return healthy; }
    }
    
    /**
     * Batch operation with query parameters.
     * 
     * POST /api/cache/batch?validate=true&async=false
     */
    @POST
    @Path("/batch/advanced")
    public Response processBatchAdvanced(
            List<WALEntry> entries,
            @QueryParam("validate") @DefaultValue("true") boolean validate,
            @QueryParam("async") @DefaultValue("false") boolean async) {
        
        logger.debug("Advanced batch request: validate={}, async={}", validate, async);
        
        // Validation
        if (validate) {
            String validationError = validateEntries(entries);
            if (validationError != null) {
                CacheOperationResponse errorResponse = CacheOperationResponse.failure(
                    "VALIDATION_ERROR", 
                    "Validation failed: " + validationError
                );
                return Response.status(Response.Status.BAD_REQUEST)
                              .entity(errorResponse)
                              .build();
            }
        }
        
        // Process (async not implemented in this example)
        return processBatch(entries);
    }
    
    /**
     * Validate entries before processing.
     */
    private String validateEntries(List<WALEntry> entries) {
        if (entries == null) {
            return "Entries cannot be null";
        }
        if (entries.isEmpty()) {
            return "Entries cannot be empty";
        }
        
        // Validate each entry
        for (WALEntry entry : entries) {
            if (entry.getTableName() == null || entry.getTableName().isEmpty()) {
                return "Table name is required for all entries";
            }
            if (entry.getOperationType() == null) {
                return "Operation type is required for all entries";
            }
        }
        
        return null; // Valid
    }
}