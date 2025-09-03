package com.telcobright.api;

import java.io.Serializable;

/**
 * Simple response object for cache operations.
 * Part of the public API - kept minimal and clean.
 */
public class CacheOperationResponse implements Serializable {
    
    private final String transactionId;
    private final boolean success;
    private final int operationsProcessed;
    private final String errorMessage;
    
    private CacheOperationResponse(String transactionId, boolean success, 
                                  int operationsProcessed, String errorMessage) {
        this.transactionId = transactionId;
        this.success = success;
        this.operationsProcessed = operationsProcessed;
        this.errorMessage = errorMessage;
    }
    
    // Factory methods for clean creation
    
    public static CacheOperationResponse success(String transactionId, int operationsProcessed) {
        return new CacheOperationResponse(transactionId, true, operationsProcessed, null);
    }
    
    public static CacheOperationResponse failure(String transactionId, String errorMessage) {
        return new CacheOperationResponse(transactionId, false, 0, errorMessage);
    }
    
    // Simple getters - no setters, immutable
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public int getOperationsProcessed() {
        return operationsProcessed;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    @Override
    public String toString() {
        return String.format("CacheOperationResponse{txn=%s, success=%s, processed=%d}",
            transactionId, success, operationsProcessed);
    }
}