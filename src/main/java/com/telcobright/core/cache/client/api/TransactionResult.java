package com.telcobright.core.cache.client.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of a transaction execution.
 */
public class TransactionResult implements Serializable {
    
    private final String transactionId;
    private final boolean success;
    private final List<OperationResult> operationResults;
    private final String errorMessage;
    private final long executionTimeMs;
    
    private TransactionResult(Builder builder) {
        this.transactionId = builder.transactionId;
        this.success = builder.success;
        this.operationResults = builder.operationResults;
        this.errorMessage = builder.errorMessage;
        this.executionTimeMs = builder.executionTimeMs;
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
    
    public List<OperationResult> getOperationResults() {
        return operationResults;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    // Builder
    
    public static class Builder {
        private String transactionId;
        private boolean success;
        private List<OperationResult> operationResults = new ArrayList<>();
        private String errorMessage;
        private long executionTimeMs;
        
        public Builder transactionId(String transactionId) {
            this.transactionId = transactionId;
            return this;
        }
        
        public Builder success(boolean success) {
            this.success = success;
            return this;
        }
        
        public Builder addOperationResult(OperationResult result) {
            this.operationResults.add(result);
            return this;
        }
        
        public Builder operationResults(List<OperationResult> results) {
            this.operationResults = results;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public Builder executionTimeMs(long executionTimeMs) {
            this.executionTimeMs = executionTimeMs;
            return this;
        }
        
        public TransactionResult build() {
            return new TransactionResult(this);
        }
    }
    
    /**
     * Result of a single operation within a transaction.
     */
    public static class OperationResult implements Serializable {
        private final int operationIndex;
        private final boolean success;
        private final Object result;
        private final String error;
        
        public OperationResult(int operationIndex, boolean success, Object result, String error) {
            this.operationIndex = operationIndex;
            this.success = success;
            this.result = result;
            this.error = error;
        }
        
        public int getOperationIndex() {
            return operationIndex;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public Object getResult() {
            return result;
        }
        
        public String getError() {
            return error;
        }
    }
}