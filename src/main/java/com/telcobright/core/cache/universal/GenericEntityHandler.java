package com.telcobright.core.cache.universal;

import com.telcobright.core.wal.WALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic implementation of EntityCacheHandler that can be extended for specific entities.
 * Provides basic cache operations and can be customized through inheritance.
 * 
 * @param <K> The key type for the entity
 * @param <V> The entity value type
 */
public abstract class GenericEntityHandler<K, V> implements EntityCacheHandler {
    private static final Logger logger = LoggerFactory.getLogger(GenericEntityHandler.class);
    
    // Cache structure: dbName -> (key -> entity)
    protected final ConcurrentHashMap<String, ConcurrentHashMap<K, V>> cache;
    
    // Table information
    protected final String tableName;
    protected final Class<V> entityType;
    protected final String keyFieldName;
    
    // Statistics
    protected final AtomicLong cacheHits = new AtomicLong(0);
    protected final AtomicLong cacheMisses = new AtomicLong(0);
    protected final AtomicLong cacheEvictions = new AtomicLong(0);
    
    public GenericEntityHandler(String tableName, Class<V> entityType, String keyFieldName) {
        this.tableName = tableName;
        this.entityType = entityType;
        this.keyFieldName = keyFieldName;
        this.cache = new ConcurrentHashMap<>();
    }
    
    @Override
    public String getTableName() {
        return tableName;
    }
    
    @Override
    public Class<?> getEntityType() {
        return entityType;
    }
    
    @Override
    public void applyToCache(String dbName, WALEntry entry) {
        ConcurrentHashMap<K, V> dbCache = cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>());
        
        try {
            switch (entry.getOperationType()) {
                case INSERT:
                case UPDATE:
                case UPSERT:
                    // Extract key and entity from WAL entry
                    K key = extractKey(entry);
                    V entity = convertToEntity(entry);
                    if (key != null && entity != null) {
                        dbCache.put(key, entity);
                        logger.trace("Applied {} to cache: db={}, table={}, key={}", 
                                   entry.getOperationType(), dbName, tableName, key);
                    }
                    break;
                    
                case DELETE:
                    K deleteKey = extractKey(entry);
                    if (deleteKey != null) {
                        dbCache.remove(deleteKey);
                        logger.trace("Applied DELETE to cache: db={}, table={}, key={}", 
                                   dbName, tableName, deleteKey);
                    }
                    break;
                    
                default:
                    logger.warn("Unknown operation type: {}", entry.getOperationType());
            }
        } catch (Exception e) {
            logger.error("Failed to apply entry to cache: {}", entry, e);
        }
    }
    
    @Override
    public void applyToDatabase(String dbName, WALEntry entry, Connection connection) throws Exception {
        switch (entry.getOperationType()) {
            case INSERT:
                executeInsert(dbName, entry, connection);
                break;
            case UPDATE:
                executeUpdate(dbName, entry, connection);
                break;
            case UPSERT:
                executeUpsert(dbName, entry, connection);
                break;
            case DELETE:
                executeDelete(dbName, entry, connection);
                break;
            default:
                throw new UnsupportedOperationException("Operation not supported: " + entry.getOperationType());
        }
    }
    
    @Override
    public Object get(String dbName, Object key) {
        ConcurrentHashMap<K, V> dbCache = cache.get(dbName);
        if (dbCache != null) {
            V value = dbCache.get(key);
            if (value != null) {
                cacheHits.incrementAndGet();
                return value;
            }
        }
        cacheMisses.incrementAndGet();
        return null;
    }
    
    @Override
    public Map<?, ?> getAll(String dbName) {
        ConcurrentHashMap<K, V> dbCache = cache.get(dbName);
        return dbCache != null ? new HashMap<>(dbCache) : new HashMap<>();
    }
    
    @Override
    public boolean contains(String dbName, Object key) {
        ConcurrentHashMap<K, V> dbCache = cache.get(dbName);
        return dbCache != null && dbCache.containsKey(key);
    }
    
    @Override
    public int size(String dbName) {
        ConcurrentHashMap<K, V> dbCache = cache.get(dbName);
        return dbCache != null ? dbCache.size() : 0;
    }
    
    @Override
    public void clear(String dbName) {
        ConcurrentHashMap<K, V> dbCache = cache.get(dbName);
        if (dbCache != null) {
            int size = dbCache.size();
            dbCache.clear();
            cacheEvictions.addAndGet(size);
        }
    }
    
    @Override
    public void initialize() {
        logger.info("Initializing handler for table: {}", tableName);
        // Can be overridden to load initial data
    }
    
    @Override
    public void shutdown() {
        logger.info("Shutting down handler for table: {}", tableName);
        cache.clear();
    }
    
    @Override
    public CacheHandlerStats getStats() {
        long totalEntries = cache.values().stream()
            .mapToLong(map -> map.size())
            .sum();
        
        return new CacheHandlerStats(
            tableName,
            totalEntries,
            cacheHits.get(),
            cacheMisses.get(),
            cacheEvictions.get()
        );
    }
    
    /**
     * Extract the key from a WAL entry.
     * Must be implemented by subclasses.
     */
    protected abstract K extractKey(WALEntry entry);
    
    /**
     * Convert WAL entry data to entity object.
     * Must be implemented by subclasses.
     */
    protected abstract V convertToEntity(WALEntry entry);
    
    /**
     * Execute INSERT operation.
     * Can be overridden for custom implementation.
     */
    protected void executeInsert(String dbName, WALEntry entry, Connection connection) throws Exception {
        // Build dynamic INSERT statement
        Map<String, Object> data = entry.getData();
        if (data.isEmpty()) {
            throw new IllegalArgumentException("No data in WAL entry for INSERT");
        }
        
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        StringBuilder values = new StringBuilder(" VALUES (");
        
        boolean first = true;
        for (String column : data.keySet()) {
            if (!first) {
                sql.append(", ");
                values.append(", ");
            }
            sql.append(column);
            values.append("?");
            first = false;
        }
        sql.append(")").append(values).append(")");
        
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (Object value : data.values()) {
                ps.setObject(index++, value);
            }
            ps.executeUpdate();
        }
    }
    
    /**
     * Execute UPDATE operation.
     * Can be overridden for custom implementation.
     */
    protected void executeUpdate(String dbName, WALEntry entry, Connection connection) throws Exception {
        Map<String, Object> data = entry.getData();
        K key = extractKey(entry);
        
        if (key == null) {
            throw new IllegalArgumentException("No key found in WAL entry for UPDATE");
        }
        
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(tableName).append(" SET ");
        
        boolean first = true;
        for (String column : data.keySet()) {
            if (!column.equals(keyFieldName)) {
                if (!first) sql.append(", ");
                sql.append(column).append(" = ?");
                first = false;
            }
        }
        sql.append(" WHERE ").append(keyFieldName).append(" = ?");
        
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (Map.Entry<String, Object> entry2 : data.entrySet()) {
                if (!entry2.getKey().equals(keyFieldName)) {
                    ps.setObject(index++, entry2.getValue());
                }
            }
            ps.setObject(index, key);
            ps.executeUpdate();
        }
    }
    
    /**
     * Execute UPSERT operation.
     * Default implementation uses INSERT ... ON DUPLICATE KEY UPDATE (MySQL).
     * Override for other databases.
     */
    protected void executeUpsert(String dbName, WALEntry entry, Connection connection) throws Exception {
        Map<String, Object> data = entry.getData();
        
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        StringBuilder values = new StringBuilder(" VALUES (");
        StringBuilder update = new StringBuilder(" ON DUPLICATE KEY UPDATE ");
        
        boolean first = true;
        for (String column : data.keySet()) {
            if (!first) {
                sql.append(", ");
                values.append(", ");
                update.append(", ");
            }
            sql.append(column);
            values.append("?");
            update.append(column).append(" = VALUES(").append(column).append(")");
            first = false;
        }
        sql.append(")").append(values).append(")").append(update);
        
        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            int index = 1;
            for (Object value : data.values()) {
                ps.setObject(index++, value);
            }
            ps.executeUpdate();
        }
    }
    
    /**
     * Execute DELETE operation.
     * Can be overridden for custom implementation.
     */
    protected void executeDelete(String dbName, WALEntry entry, Connection connection) throws Exception {
        K key = extractKey(entry);
        if (key == null) {
            throw new IllegalArgumentException("No key found in WAL entry for DELETE");
        }
        
        String sql = "DELETE FROM " + tableName + " WHERE " + keyFieldName + " = ?";
        
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setObject(1, key);
            ps.executeUpdate();
        }
    }
}