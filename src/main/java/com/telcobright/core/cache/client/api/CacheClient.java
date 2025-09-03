package com.telcobright.core.cache.client.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Unified interface for cache client operations.
 * Supports gRPC, REST, and embedded (direct) access patterns.
 * 
 * @param <K> Key type
 * @param <V> Value type
 */
public interface CacheClient<K, V> {
    
    /**
     * Get a single value by key.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @return Value or null if not found
     */
    V get(String database, String table, K key);
    
    /**
     * Get a single value asynchronously.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @return CompletableFuture with value or null
     */
    CompletableFuture<V> getAsync(String database, String table, K key);
    
    /**
     * Get multiple values by keys (batch get).
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param keys List of keys
     * @return Map of key to value
     */
    Map<K, V> getBatch(String database, String table, List<K> keys);
    
    /**
     * Get all values for a table.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @return Map of all key-value pairs
     */
    Map<K, V> getAll(String database, String table);
    
    /**
     * Put a single value.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @param value Value to store
     * @return Previous value or null
     */
    V put(String database, String table, K key, V value);
    
    /**
     * Put a single value asynchronously.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @param value Value to store
     * @return CompletableFuture with previous value or null
     */
    CompletableFuture<V> putAsync(String database, String table, K key, V value);
    
    /**
     * Put multiple values (batch put).
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param entries Map of key-value pairs
     */
    void putBatch(String database, String table, Map<K, V> entries);
    
    /**
     * Update an existing value.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @param value Updated value
     * @return true if updated, false if key doesn't exist
     */
    boolean update(String database, String table, K key, V value);
    
    /**
     * Delete a value.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @return Deleted value or null
     */
    V delete(String database, String table, K key);
    
    /**
     * Delete multiple values (batch delete).
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param keys List of keys to delete
     * @return Map of deleted key-value pairs
     */
    Map<K, V> deleteBatch(String database, String table, List<K> keys);
    
    /**
     * Check if key exists.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param key Primary key
     * @return true if exists
     */
    boolean contains(String database, String table, K key);
    
    /**
     * Get size of a table cache.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @return Number of entries
     */
    int size(String database, String table);
    
    /**
     * Clear all entries in a table.
     * 
     * @param database Database name
     * @param table Table/cache name
     */
    void clear(String database, String table);
    
    /**
     * Execute a query with filter.
     * 
     * @param database Database name
     * @param table Table/cache name
     * @param filter Query filter
     * @return Filtered results
     */
    List<V> query(String database, String table, QueryFilter<V> filter);
    
    /**
     * Close the client and release resources.
     */
    void close();
    
    /**
     * Check if client is connected/healthy.
     * 
     * @return true if healthy
     */
    boolean isHealthy();
    
    /**
     * Get client type.
     * 
     * @return Client type (GRPC, REST, EMBEDDED)
     */
    ClientType getType();
    
    /**
     * Client types.
     */
    enum ClientType {
        GRPC,
        REST,
        EMBEDDED
    }
}