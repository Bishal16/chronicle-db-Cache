package com.telcobright.core.cache.api;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Cache statistics and metrics.
 */
public class CacheStatistics implements Serializable {
    
    private final long totalOperations;
    private final long successfulOperations;
    private final long failedOperations;
    private final long cacheHits;
    private final long cacheMisses;
    private final long totalEntries;
    private final long walQueueSize;
    private final double averageResponseTimeMs;
    private final LocalDateTime startTime;
    private final LocalDateTime currentTime;
    
    private CacheStatistics(Builder builder) {
        this.totalOperations = builder.totalOperations;
        this.successfulOperations = builder.successfulOperations;
        this.failedOperations = builder.failedOperations;
        this.cacheHits = builder.cacheHits;
        this.cacheMisses = builder.cacheMisses;
        this.totalEntries = builder.totalEntries;
        this.walQueueSize = builder.walQueueSize;
        this.averageResponseTimeMs = builder.averageResponseTimeMs;
        this.startTime = builder.startTime;
        this.currentTime = builder.currentTime;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public long getTotalOperations() { return totalOperations; }
    public long getSuccessfulOperations() { return successfulOperations; }
    public long getFailedOperations() { return failedOperations; }
    public long getCacheHits() { return cacheHits; }
    public long getCacheMisses() { return cacheMisses; }
    public long getTotalEntries() { return totalEntries; }
    public long getWalQueueSize() { return walQueueSize; }
    public double getAverageResponseTimeMs() { return averageResponseTimeMs; }
    public LocalDateTime getStartTime() { return startTime; }
    public LocalDateTime getCurrentTime() { return currentTime; }
    
    public double getCacheHitRate() {
        long total = cacheHits + cacheMisses;
        return total == 0 ? 0 : (double) cacheHits / total;
    }
    
    public double getSuccessRate() {
        return totalOperations == 0 ? 0 : (double) successfulOperations / totalOperations;
    }
    
    // Builder
    public static class Builder {
        private long totalOperations;
        private long successfulOperations;
        private long failedOperations;
        private long cacheHits;
        private long cacheMisses;
        private long totalEntries;
        private long walQueueSize;
        private double averageResponseTimeMs;
        private LocalDateTime startTime;
        private LocalDateTime currentTime = LocalDateTime.now();
        
        public Builder totalOperations(long totalOperations) {
            this.totalOperations = totalOperations;
            return this;
        }
        
        public Builder successfulOperations(long successfulOperations) {
            this.successfulOperations = successfulOperations;
            return this;
        }
        
        public Builder failedOperations(long failedOperations) {
            this.failedOperations = failedOperations;
            return this;
        }
        
        public Builder cacheHits(long cacheHits) {
            this.cacheHits = cacheHits;
            return this;
        }
        
        public Builder cacheMisses(long cacheMisses) {
            this.cacheMisses = cacheMisses;
            return this;
        }
        
        public Builder totalEntries(long totalEntries) {
            this.totalEntries = totalEntries;
            return this;
        }
        
        public Builder walQueueSize(long walQueueSize) {
            this.walQueueSize = walQueueSize;
            return this;
        }
        
        public Builder averageResponseTimeMs(double averageResponseTimeMs) {
            this.averageResponseTimeMs = averageResponseTimeMs;
            return this;
        }
        
        public Builder startTime(LocalDateTime startTime) {
            this.startTime = startTime;
            return this;
        }
        
        public Builder currentTime(LocalDateTime currentTime) {
            this.currentTime = currentTime;
            return this;
        }
        
        public CacheStatistics build() {
            return new CacheStatistics(this);
        }
    }
}