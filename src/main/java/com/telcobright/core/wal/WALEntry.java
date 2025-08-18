package com.telcobright.core.wal;

import java.util.HashMap;
import java.util.Map;

/**
 * Generic WAL (Write-Ahead Log) entry that can represent any database operation.
 * This is the core data structure for all WAL operations.
 */
public class WALEntry {
    private String dbName;
    private String tableName;
    private OperationType operationType;
    private Map<String, Object> data;
    private Long timestamp;
    private String transactionId;
    
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
        
        public Builder withTransactionId(String transactionId) {
            entry.transactionId = transactionId;
            return this;
        }
        
        public WALEntry build() {
            return entry;
        }
    }
    
    // Getters and setters
    public String getDbName() {
        return dbName;
    }
    
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    public OperationType getOperationType() {
        return operationType;
    }
    
    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }
    
    public Map<String, Object> getData() {
        return data;
    }
    
    public void setData(Map<String, Object> data) {
        this.data = data;
    }
    
    public Object get(String key) {
        return data.get(key);
    }
    
    public void put(String key, Object value) {
        data.put(key, value);
    }
    
    public Long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
    
    public String getFullTableName() {
        return dbName + "." + tableName;
    }
}