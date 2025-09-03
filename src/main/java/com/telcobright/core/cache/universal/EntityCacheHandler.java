package com.telcobright.core.cache.universal;

import com.telcobright.core.wal.WALEntry;
import java.sql.Connection;
import java.util.Map;

/**
 * Interface for entity-specific cache handlers.
 * Each entity type (table) should have its own implementation.
 * Handlers are responsible for:
 * 1. Managing in-memory cache for their entity type
 * 2. Applying WAL entries to the cache
 * 3. Persisting changes to database (when needed)
 * 4. Handling entity-specific business logic
 */
public interface EntityCacheHandler {
    
    /**
     * Get the table name this handler manages
     */
    String getTableName();
    
    /**
     * Get the entity type/class this handler manages
     */
    Class<?> getEntityType();
    
    /**
     * Apply a WAL entry to the in-memory cache.
     * This is called during normal operation and replay.
     * 
     * @param dbName The database name
     * @param entry The WAL entry to apply
     */
    void applyToCache(String dbName, WALEntry entry);
    
    /**
     * Apply a WAL entry to the database.
     * Used during batch processing with transactions.
     * 
     * @param dbName The database name
     * @param entry The WAL entry to apply
     * @param connection The database connection (for transaction management)
     */
    void applyToDatabase(String dbName, WALEntry entry, Connection connection) throws Exception;
    
    /**
     * Get an entity from cache by key
     * 
     * @param dbName The database name
     * @param key The entity key
     * @return The cached entity or null if not found
     */
    Object get(String dbName, Object key);
    
    /**
     * Get all entities from cache for a database
     * 
     * @param dbName The database name
     * @return Map of key to entity
     */
    Map<?, ?> getAll(String dbName);
    
    /**
     * Check if an entity exists in cache
     * 
     * @param dbName The database name
     * @param key The entity key
     * @return true if entity exists in cache
     */
    boolean contains(String dbName, Object key);
    
    /**
     * Get the size of cache for a database
     * 
     * @param dbName The database name
     * @return Number of entities cached
     */
    int size(String dbName);
    
    /**
     * Clear the cache for a database
     * 
     * @param dbName The database name
     */
    void clear(String dbName);
    
    /**
     * Initialize the handler (load initial data, setup connections, etc.)
     */
    void initialize();
    
    /**
     * Shutdown the handler (cleanup resources)
     */
    void shutdown();
    
    /**
     * Validate if this handler can process the given WAL entry
     * 
     * @param entry The WAL entry to check
     * @return true if this handler can process the entry
     */
    default boolean canHandle(WALEntry entry) {
        return getTableName().equalsIgnoreCase(entry.getTableName());
    }
    
    /**
     * Get statistics about this cache handler
     */
    default CacheHandlerStats getStats() {
        return new CacheHandlerStats(getTableName(), 0, 0, 0, 0);
    }
    
    /**
     * Statistics class for monitoring
     */
    class CacheHandlerStats {
        public final String tableName;
        public final long totalEntries;
        public final long hits;
        public final long misses;
        public final long evictions;
        
        public CacheHandlerStats(String tableName, long totalEntries, long hits, long misses, long evictions) {
            this.tableName = tableName;
            this.totalEntries = totalEntries;
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
        }
    }
}