package com.telcobright.core.cache.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Result of a cache query operation.
 */
public class CacheQueryResult implements Serializable {
    
    private final boolean success;
    private final List<Object> results;
    private final int totalCount;
    private final long executionTimeMs;
    private final String errorMessage;
    
    private CacheQueryResult(Builder builder) {
        this.success = builder.success;
        this.results = builder.results;
        this.totalCount = builder.totalCount;
        this.executionTimeMs = builder.executionTimeMs;
        this.errorMessage = builder.errorMessage;
    }
    
    // Factory methods
    
    public static CacheQueryResult single(Object result) {
        return builder()
            .success(true)
            .addResult(result)
            .totalCount(result != null ? 1 : 0)
            .build();
    }
    
    public static CacheQueryResult multiple(List<Object> results) {
        return builder()
            .success(true)
            .results(results)
            .totalCount(results.size())
            .build();
    }
    
    public static CacheQueryResult empty() {
        return builder()
            .success(true)
            .totalCount(0)
            .build();
    }
    
    public static CacheQueryResult error(String errorMessage) {
        return builder()
            .success(false)
            .errorMessage(errorMessage)
            .build();
    }
    
    public static CacheQueryResult count(int count) {
        return builder()
            .success(true)
            .totalCount(count)
            .build();
    }
    
    public static CacheQueryResult exists(boolean exists) {
        return builder()
            .success(true)
            .addResult(exists)
            .totalCount(exists ? 1 : 0)
            .build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    
    public boolean isSuccess() {
        return success;
    }
    
    public List<Object> getResults() {
        return results;
    }
    
    public Object getFirstResult() {
        return results.isEmpty() ? null : results.get(0);
    }
    
    @SuppressWarnings("unchecked")
    public <T> T getTypedResult(Class<T> type) {
        Object first = getFirstResult();
        return first != null && type.isInstance(first) ? (T) first : null;
    }
    
    @SuppressWarnings("unchecked")
    public <T> List<T> getTypedResults(Class<T> type) {
        List<T> typedResults = new ArrayList<>();
        for (Object result : results) {
            if (type.isInstance(result)) {
                typedResults.add((T) result);
            }
        }
        return typedResults;
    }
    
    public int getTotalCount() {
        return totalCount;
    }
    
    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }
    
    public boolean isEmpty() {
        return results.isEmpty();
    }
    
    public boolean hasResults() {
        return !results.isEmpty();
    }
    
    // Builder
    
    public static class Builder {
        private boolean success;
        private List<Object> results = new ArrayList<>();
        private int totalCount;
        private long executionTimeMs;
        private String errorMessage;
        
        public Builder success(boolean success) {
            this.success = success;
            return this;
        }
        
        public Builder addResult(Object result) {
            if (result != null) {
                this.results.add(result);
            }
            return this;
        }
        
        public Builder results(List<Object> results) {
            this.results = results != null ? results : new ArrayList<>();
            return this;
        }
        
        public Builder totalCount(int totalCount) {
            this.totalCount = totalCount;
            return this;
        }
        
        public Builder executionTimeMs(long executionTimeMs) {
            this.executionTimeMs = executionTimeMs;
            return this;
        }
        
        public Builder errorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
            return this;
        }
        
        public CacheQueryResult build() {
            return new CacheQueryResult(this);
        }
    }
    
    @Override
    public String toString() {
        return String.format("CacheQueryResult{success=%s, count=%d, time=%dms}",
            success, totalCount, executionTimeMs);
    }
}