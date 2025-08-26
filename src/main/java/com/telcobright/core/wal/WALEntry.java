package com.telcobright.core.wal;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic WAL (Write-Ahead Log) entry that can represent any database operation.
 * This is the core data structure for all WAL operations.
 */
@Setter
@Getter
public class WALEntry {
    // Getters and setters
    private String dbName;
    private String tableName;
    private OperationType operationType;
    private Map<String, Object> data;
    private Long timestamp;
    // transactionId removed - now managed at WALEntryBatch level
    @Deprecated
    private transient String transactionId; // Kept for backward compatibility, not serialized
    
    public enum OperationType {
        INSERT,
        UPDATE,
        DELETE,
        UPSERT,
        BATCH
    }
    
    public WALEntry() {
        this.data = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
    }
    
    public WALEntry(String dbName, String tableName, OperationType operationType) {
        this();
        this.dbName = dbName;
        this.tableName = tableName;
        this.operationType = operationType;
    }
    
    // Static builder method
    public static Builder builder() {
        return new Builder();
    }
    
    // Builder pattern for fluent API
    public static class Builder {
        private final WALEntry entry;
        
        public Builder() {
            entry = new WALEntry();
        }
        
        public Builder(String dbName, String tableName, OperationType operationType) {
            entry = new WALEntry(dbName, tableName, operationType);
        }
        
        public Builder dbName(String dbName) {
            entry.dbName = dbName;
            return this;
        }
        
        public Builder tableName(String tableName) {
            entry.tableName = tableName;
            return this;
        }
        
        public Builder operationType(OperationType operationType) {
            entry.operationType = operationType;
            return this;
        }
        
        public Builder data(Map<String, Object> data) {
            entry.data = data;
            return this;
        }
        
        public Builder withData(String key, Object value) {
            entry.data.put(key, value);
            return this;
        }
        
        public Builder withData(Map<String, Object> data) {
            entry.data.putAll(data);
            return this;
        }
        
        /**
         * @deprecated Use WALEntryBatch.builder().transactionId() instead
         */
        @Deprecated
        public Builder withTransactionId(String transactionId) {
            entry.transactionId = transactionId;
            return this;
        }
        
        public WALEntry build() {
            return entry;
        }
    }

    public Object get(String key) {
        return data.get(key);
    }
    
    public void put(String key, Object value) {
        data.put(key, value);
    }

    public String getFullTableName() {
        return dbName + "." + tableName;
    }
}