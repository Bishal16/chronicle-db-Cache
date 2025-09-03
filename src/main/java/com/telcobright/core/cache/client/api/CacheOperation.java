package com.telcobright.core.cache.client.api;

import java.io.Serializable;

/**
 * Represents a cache operation for transactional execution.
 */
public class CacheOperation<K, V> implements Serializable {
    
    public enum OperationType {
        GET,
        PUT,
        UPDATE,
        DELETE,
        CONTAINS
    }
    
    private final OperationType type;
    private final String database;
    private final String table;
    private final K key;
    private final V value;
    
    private CacheOperation(OperationType type, String database, String table, K key, V value) {
        this.type = type;
        this.database = database;
        this.table = table;
        this.key = key;
        this.value = value;
    }
    
    // Static factory methods
    
    public static <K, V> CacheOperation<K, V> get(String database, String table, K key) {
        return new CacheOperation<>(OperationType.GET, database, table, key, null);
    }
    
    public static <K, V> CacheOperation<K, V> put(String database, String table, K key, V value) {
        return new CacheOperation<>(OperationType.PUT, database, table, key, value);
    }
    
    public static <K, V> CacheOperation<K, V> update(String database, String table, K key, V value) {
        return new CacheOperation<>(OperationType.UPDATE, database, table, key, value);
    }
    
    public static <K, V> CacheOperation<K, V> delete(String database, String table, K key) {
        return new CacheOperation<>(OperationType.DELETE, database, table, key, null);
    }
    
    public static <K, V> CacheOperation<K, V> contains(String database, String table, K key) {
        return new CacheOperation<>(OperationType.CONTAINS, database, table, key, null);
    }
    
    // Getters
    
    public OperationType getType() {
        return type;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getTable() {
        return table;
    }
    
    public K getKey() {
        return key;
    }
    
    public V getValue() {
        return value;
    }
    
    @Override
    public String toString() {
        return String.format("CacheOperation{type=%s, db=%s, table=%s, key=%s}", 
                           type, database, table, key);
    }
}