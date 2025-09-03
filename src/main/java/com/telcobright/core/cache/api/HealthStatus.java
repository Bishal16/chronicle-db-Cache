package com.telcobright.core.cache.api;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Health status of the cache system.
 */
public class HealthStatus implements Serializable {
    
    public enum Status {
        HEALTHY,
        DEGRADED,
        UNHEALTHY,
        UNKNOWN
    }
    
    private final Status status;
    private final boolean walHealthy;
    private final boolean cacheHealthy;
    private final boolean databaseHealthy;
    private final List<HealthCheck> checks;
    private final LocalDateTime timestamp;
    
    private HealthStatus(Builder builder) {
        this.status = builder.status;
        this.walHealthy = builder.walHealthy;
        this.cacheHealthy = builder.cacheHealthy;
        this.databaseHealthy = builder.databaseHealthy;
        this.checks = builder.checks;
        this.timestamp = builder.timestamp;
    }
    
    public static HealthStatus healthy() {
        return builder()
            .status(Status.HEALTHY)
            .walHealthy(true)
            .cacheHealthy(true)
            .databaseHealthy(true)
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    public static HealthStatus degraded(String reason) {
        return builder()
            .status(Status.DEGRADED)
            .addCheck(new HealthCheck("degraded", false, reason))
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    public static HealthStatus unhealthy(String reason) {
        return builder()
            .status(Status.UNHEALTHY)
            .addCheck(new HealthCheck("unhealthy", false, reason))
            .timestamp(LocalDateTime.now())
            .build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public Status getStatus() { return status; }
    public boolean isWalHealthy() { return walHealthy; }
    public boolean isCacheHealthy() { return cacheHealthy; }
    public boolean isDatabaseHealthy() { return databaseHealthy; }
    public List<HealthCheck> getChecks() { return checks; }
    public LocalDateTime getTimestamp() { return timestamp; }
    
    public boolean isHealthy() {
        return status == Status.HEALTHY;
    }
    
    public boolean isDegraded() {
        return status == Status.DEGRADED;
    }
    
    public boolean isUnhealthy() {
        return status == Status.UNHEALTHY;
    }
    
    // Builder
    public static class Builder {
        private Status status = Status.UNKNOWN;
        private boolean walHealthy = true;
        private boolean cacheHealthy = true;
        private boolean databaseHealthy = true;
        private List<HealthCheck> checks = new ArrayList<>();
        private LocalDateTime timestamp = LocalDateTime.now();
        
        public Builder status(Status status) {
            this.status = status;
            return this;
        }
        
        public Builder walHealthy(boolean walHealthy) {
            this.walHealthy = walHealthy;
            return this;
        }
        
        public Builder cacheHealthy(boolean cacheHealthy) {
            this.cacheHealthy = cacheHealthy;
            return this;
        }
        
        public Builder databaseHealthy(boolean databaseHealthy) {
            this.databaseHealthy = databaseHealthy;
            return this;
        }
        
        public Builder addCheck(HealthCheck check) {
            this.checks.add(check);
            return this;
        }
        
        public Builder checks(List<HealthCheck> checks) {
            this.checks = checks;
            return this;
        }
        
        public Builder timestamp(LocalDateTime timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public HealthStatus build() {
            // Auto-determine status based on component health
            if (!walHealthy || !cacheHealthy || !databaseHealthy) {
                if (status == Status.UNKNOWN || status == Status.HEALTHY) {
                    status = Status.DEGRADED;
                }
            }
            return new HealthStatus(this);
        }
    }
    
    /**
     * Individual health check result.
     */
    public static class HealthCheck implements Serializable {
        private final String name;
        private final boolean passed;
        private final String message;
        
        public HealthCheck(String name, boolean passed, String message) {
            this.name = name;
            this.passed = passed;
            this.message = message;
        }
        
        public String getName() { return name; }
        public boolean isPassed() { return passed; }
        public String getMessage() { return message; }
    }
    
    @Override
    public String toString() {
        return String.format("HealthStatus{status=%s, wal=%s, cache=%s, db=%s}",
            status, walHealthy, cacheHealthy, databaseHealthy);
    }
}