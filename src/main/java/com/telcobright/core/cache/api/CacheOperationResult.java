package com.telcobright.core.cache.api;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of a cache operation (single or batch).
 * Contains success status, affected entities, and any errors.
 */
public class CacheOperationResult implements Serializable {
    
    private final String transactionId;
    private final boolean success;
    private final int operationsProcessed;
    private final int operationsFailed;
    private final List<EntityResult> entityResults;
    private final String errorMessage;
    private final LocalDateTime timestamp;
    private final long executionTimeMs;
    
    private CacheOperationResult(Builder builder) {
        this.transactionId = builder.transactionId;
        this.success = builder.success;
        this.operationsProcessed = builder.operationsProcessed;
        this.operationsFailed = builder.operationsFailed;
        this.entityResults = builder.entityResults;
        this.errorMessage = builder.errorMessage;
        this.timestamp = builder.timestamp;
        this.executionTimeMs = builder.executionTimeMs;
    }
    
    // Success factory method
    public static CacheOperationResult success(String transactionId, int operationsProcessed) {
        return builder()
            .transactionId(transactionId)
            .success(true)
            .operationsProcessed(operationsProcessed)
            .operationsFailed(0)
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    // Failure factory method
    public static CacheOperationResult failure(String transactionId, String errorMessage) {
        return builder()
            .transactionId(transactionId)
            .success(false)
            .operationsProcessed(0)
            .operationsFailed(0)
            .errorMessage(errorMessage)
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    // Partial success factory method
    public static CacheOperationResult partial(String transactionId, int processed, int failed) {
        return builder()
            .transactionId(transactionId)
            .success(false)
            .operationsProcessed(processed)
            .operationsFailed(failed)
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public String getTransactionId() {
        return transactionId;
    }
    
    public boolean isSuccess() {
        return success;
    }
    
    public int getOperationsProcessed() {
        return operationsProcessed;
    }
    
    public int getOperationsFailed() {
        return operationsFailed;
    }
    
    public List<EntityResult> getEntityResults() {
        return entityResults;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    public int getTotalOperations() {
        return operationsProcessed + operationsFailed;
    }
    
    // Builder
    public static class Builder {
        private String transactionId;
        private boolean success;
        private int operationsProcessed;
        private int operationsFailed;
        private List<EntityResult> entityResults = new ArrayList<>();
        private String errorMessage;
        private LocalDateTime timestamp = LocalDateTime.now();
        private long executionTimeMs;
        
        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }
        
        public Builder success(boolean success) {
            this.success = success;
            return this;
        }
        
        public Builder operationsProcessed(int operationsProcessed) {
            this.operationsProcessed = operationsProcessed;
            return this;
        }
        
        public Builder operationsFailed(int operationsFailed) {
            this.operationsFailed = operationsFailed;
            return this;
        }
        
        public Builder addEntityResult(EntityResult result) {
            this.entityResults.add(result);
            return this;
        }
        
        public Builder entityResults(List<EntityResult> entityResults) {
            this.entityResults = entityResults;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder timestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public Builder executionTimeMs(long executionTimeMs) {
            this.executionTimeMs = executionTimeMs;
            return this;
        }
        
        public CacheOperationResult build() {
            return new CacheOperationResult(this);
        }
    }
    
    /**
     * Result for a specific entity operation.
     */
    public static class EntityResult implements Serializable {
        private final String database;
        private final String table;
        private final String operation;
        private final Object key;
        private final boolean success;
        private final String error;
        
        public EntityResult(String database, String table, String operation, 
                          Object key, boolean success, String error) {
            this.database = database;
            this.table = table;
            this.operation = operation;
            this.key = key;
            this.success = success;
            this.error = error;
        }
        
        // Getters
        public String getDatabase() { return database; }
        public String getTable() { return table; }
        public String getOperation() { return operation; }
        public Object getKey() { return key; }
        public boolean isSuccess() { return success; }
        public String getError() { return error; }
    }
    
    @Override
    public String toString() {
        return String.format("CacheOperationResult{txn=%s, success=%s, processed=%d, failed=%d, time=%dms}",
            transactionId, success, operationsProcessed, operationsFailed, executionTimeMs);
    }
}