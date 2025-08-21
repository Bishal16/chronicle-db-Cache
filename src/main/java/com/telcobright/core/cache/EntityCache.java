package com.telcobright.core.cache;

import com.telcobright.core.cache.annotations.Column;
import com.telcobright.core.cache.annotations.Table;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Abstract entity cache with structure:
 * ConcurrentHashMap<dbname, ConcurrentHashMap<TKey, TEntity>>
 * 
 * Simple, efficient, and type-safe caching for any entity type.
 * Extend this class for specific entity caches.
 * @param <K> The type of the primary key
 * @param <T> The entity type
 */
public abstract class EntityCache<K, T> {
    private static final Logger logger = LoggerFactory.getLogger(EntityCache.class);
    
    // Main cache structure: dbname -> (primaryKey -> entity)
    private final ConcurrentHashMap<String, ConcurrentHashMap<K, T>> cache = new ConcurrentHashMap<>();
    
    // Entity metadata (extracted once at construction)
    private final Class<T> entityClass;
    private final String tableName;
    private final Field primaryKeyField;
    private final String primaryKeyColumn;
    private final Map<String, Field> columnFieldMap;
    
    // Optional components
    protected DataSource dataSource;
    protected WALWriter walWriter;
    
    /**
     * Abstract method to extract the primary key from an entity
     */
    protected abstract K extractKey(T entity);
    
    /**
     * Protected constructor for subclasses
     * @param entityClass The entity class with @Table and @Column annotations
     */
    protected EntityCache(Class<T> entityClass) {
        this.entityClass = entityClass;
        
        // Extract table metadata
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Entity class " + entityClass.getName() + " must have @Table annotation");
        }
        this.tableName = tableAnnotation.name();
        
        // Build column mapping and find primary key
        this.columnFieldMap = new LinkedHashMap<>();
        Field pkField = null;
        String pkColumn = null;
        
        for (Field field : entityClass.getDeclaredFields()) {
            Column columnAnnotation = field.getAnnotation(Column.class);
            if (columnAnnotation != null) {
                field.setAccessible(true);
                String columnName = columnAnnotation.name();
                columnFieldMap.put(columnName, field);
                
                if (columnAnnotation.primaryKey()) {
                    if (pkField != null) {
                        throw new IllegalArgumentException("Entity " + entityClass.getName() + " has multiple primary keys");
                    }
                    pkField = field;
                    pkColumn = columnName;
                }
            }
        }
        
        if (pkField == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() + " must have a primary key field");
        }
        
        this.primaryKeyField = pkField;
        this.primaryKeyColumn = pkColumn;
        
        logger.info("Created EntityCache for {}: table={}, primaryKey={}", 
            entityClass.getSimpleName(), tableName, primaryKeyColumn);
    }
    
    /**
     * Initialize with datasource and WAL writer
     */
    public void initialize(DataSource dataSource, WALWriter walWriter) {
        this.dataSource = dataSource;
        this.walWriter = walWriter;
    }
    
    /**
     * Apply batch inserts - writes to WAL first, then updates cache
     * @param entities Map of dbName -> List of entities to insert
     * @param transactionId Transaction ID for the batch
     * @throws RuntimeException if WAL write fails
     */
    public void applyInserts(Map<String, List<T>> entities, String transactionId) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        List<WALEntry> walEntries = new ArrayList<>();
        
        // First, prepare all WAL entries
        for (Map.Entry<String, List<T>> dbEntry : entities.entrySet()) {
            String dbName = dbEntry.getKey();
            
            for (T entity : dbEntry.getValue()) {
                WALEntry walEntry = createWALEntry(dbName, entity, WALEntry.OperationType.INSERT);
                walEntry.setTransactionId(transactionId);
                walEntries.add(walEntry);
            }
        }
        
        // Write to WAL FIRST - if this fails, exception is thrown and cache is not updated
        if (!walEntries.isEmpty()) {
            if (walWriter == null) {
                throw new IllegalStateException("WALWriter not initialized");
            }
            walWriter.write(walEntries);
            logger.info("Wrote batch of {} inserts to WAL for {}, txId: {}", 
                walEntries.size(), tableName, transactionId);
        }
        
        // Only update cache after successful WAL write
        for (Map.Entry<String, List<T>> dbEntry : entities.entrySet()) {
            String dbName = dbEntry.getKey();
            ConcurrentHashMap<K, T> dbCache = getOrCreateDbCache(dbName);
            
            for (T entity : dbEntry.getValue()) {
                K key = extractKey(entity);
                dbCache.put(key, entity);
            }
        }
    }
    
    /**
     * Apply batch updates - writes to WAL first, then updates cache
     * @param entities Map of dbName -> List of entities to update
     * @param transactionId Transaction ID for the batch
     * @throws RuntimeException if WAL write fails
     */
    public void applyUpdates(Map<String, List<T>> entities, String transactionId) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        List<WALEntry> walEntries = new ArrayList<>();
        
        // First, prepare all WAL entries
        for (Map.Entry<String, List<T>> dbEntry : entities.entrySet()) {
            String dbName = dbEntry.getKey();
            
            for (T entity : dbEntry.getValue()) {
                WALEntry walEntry = createWALEntry(dbName, entity, WALEntry.OperationType.UPDATE);
                walEntry.setTransactionId(transactionId);
                walEntries.add(walEntry);
            }
        }
        
        // Write to WAL FIRST - if this fails, exception is thrown and cache is not updated
        if (!walEntries.isEmpty()) {
            if (walWriter == null) {
                throw new IllegalStateException("WALWriter not initialized");
            }
            walWriter.write(walEntries);
            logger.info("Wrote batch of {} updates to WAL for {}, txId: {}", 
                walEntries.size(), tableName, transactionId);
        }
        
        // Only update cache after successful WAL write
        for (Map.Entry<String, List<T>> dbEntry : entities.entrySet()) {
            String dbName = dbEntry.getKey();
            ConcurrentHashMap<K, T> dbCache = getOrCreateDbCache(dbName);
            
            for (T entity : dbEntry.getValue()) {
                K key = extractKey(entity);
                dbCache.put(key, entity);
            }
        }
    }
    
    /**
     * Apply batch deletes - writes to WAL first, then updates cache
     * @param keys Map of dbName -> List of keys to delete
     * @param transactionId Transaction ID for the batch
     * @throws RuntimeException if WAL write fails
     */
    public void applyDeletes(Map<String, List<K>> keys, String transactionId) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        
        List<WALEntry> walEntries = new ArrayList<>();
        Map<String, Map<K, T>> entitiesToDelete = new HashMap<>();
        
        // First, prepare all WAL entries and collect entities
        for (Map.Entry<String, List<K>> dbEntry : keys.entrySet()) {
            String dbName = dbEntry.getKey();
            ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
            
            if (dbCache != null) {
                Map<K, T> dbEntitiesToDelete = new HashMap<>();
                
                for (K key : dbEntry.getValue()) {
                    T entity = dbCache.get(key);  // Get but don't remove yet
                    
                    if (entity != null) {
                        WALEntry walEntry = createWALEntry(dbName, entity, WALEntry.OperationType.DELETE);
                        walEntry.setTransactionId(transactionId);
                        walEntries.add(walEntry);
                        dbEntitiesToDelete.put(key, entity);
                    }
                }
                
                if (!dbEntitiesToDelete.isEmpty()) {
                    entitiesToDelete.put(dbName, dbEntitiesToDelete);
                }
            }
        }
        
        // Write to WAL FIRST - if this fails, exception is thrown and cache is not updated
        if (!walEntries.isEmpty()) {
            if (walWriter == null) {
                throw new IllegalStateException("WALWriter not initialized");
            }
            walWriter.write(walEntries);
            logger.info("Wrote batch of {} deletes to WAL for {}, txId: {}", 
                walEntries.size(), tableName, transactionId);
        }
        
        // Only remove from cache after successful WAL write
        for (Map.Entry<String, Map<K, T>> dbEntry : entitiesToDelete.entrySet()) {
            String dbName = dbEntry.getKey();
            ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
            
            if (dbCache != null) {
                for (K key : dbEntry.getValue().keySet()) {
                    dbCache.remove(key);
                }
            }
        }
    }
    
    /**
     * Load all data from a specific database
     * This does NOT write to WAL since it's loading existing data
     */
    public void loadFromDatabase(String dbName) throws SQLException {
        if (dataSource == null) {
            throw new IllegalStateException("DataSource not initialized");
        }
        
        String sql = String.format("SELECT * FROM %s.%s", dbName, tableName);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            ConcurrentHashMap<K, T> dbCache = cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>());
            int count = 0;
            
            while (rs.next()) {
                T entity = mapResultSetToEntity(rs);
                K key = extractKey(entity);
                dbCache.put(key, entity);
                count++;
            }
            
            logger.info("Loaded {} entities from {}.{}", count, dbName, tableName);
        }
    }
    
    /**
     * Put an entity directly into cache without WAL write
     * Used for loading data from database or replay
     */
    protected void putDirectToCache(String dbName, T entity) {
        K key = extractKey(entity);
        cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>()).put(key, entity);
    }
    
    /**
     * Get an entity by key from a specific database
     */
    public T get(String dbName, K key) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        return dbCache != null ? dbCache.get(key) : null;
    }
    
    /**
     * Put an entity into cache for a specific database
     * Writes to WAL first, then updates cache
     * @throws RuntimeException if WAL write fails
     */
    public void put(String dbName, T entity) {
        if (walWriter == null) {
            throw new IllegalStateException("WALWriter not initialized");
        }
        
        // Write to WAL FIRST - if this fails, exception is thrown and cache is not updated
        writeToWAL(dbName, entity, WALEntry.OperationType.UPSERT);
        
        // Only update cache after successful WAL write
        K key = extractKey(entity);
        cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>()).put(key, entity);
    }
    
    /**
     * Update an entity in cache
     * Writes to WAL first, then updates cache
     * @throws RuntimeException if WAL write fails
     */
    public void update(String dbName, K key, T entity) {
        if (walWriter == null) {
            throw new IllegalStateException("WALWriter not initialized");
        }
        
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        if (dbCache != null) {
            // Write to WAL FIRST - if this fails, exception is thrown and cache is not updated
            writeToWAL(dbName, entity, WALEntry.OperationType.UPDATE);
            
            // Only update cache after successful WAL write
            dbCache.put(key, entity);
        }
    }
    
    /**
     * Delete an entity from cache
     * Writes to WAL first, then updates cache
     * @throws RuntimeException if WAL write fails
     */
    public void delete(String dbName, K key) {
        if (walWriter == null) {
            throw new IllegalStateException("WALWriter not initialized");
        }
        
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        if (dbCache != null) {
            T entity = dbCache.get(key);  // Get but don't remove yet
            
            if (entity != null) {
                // Write to WAL FIRST - if this fails, exception is thrown and cache is not updated
                writeToWAL(dbName, entity, WALEntry.OperationType.DELETE);
                
                // Only remove from cache after successful WAL write
                dbCache.remove(key);
            }
        }
    }
    
    /**
     * Get all entities for a database
     */
    public Map<K, T> getAll(String dbName) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        return dbCache != null ? new HashMap<>(dbCache) : new HashMap<>();
    }
    
    /**
     * Get or create database cache
     */
    public ConcurrentHashMap<K, T> getOrCreateDbCache(String dbName) {
        return cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>());
    }
    
    /**
     * Check if entity exists
     */
    public boolean contains(String dbName, K key) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        return dbCache != null && dbCache.containsKey(key);
    }
    
    /**
     * Clear cache for a specific database
     */
    public void clear(String dbName) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        if (dbCache != null) {
            dbCache.clear();
        }
    }
    
    /**
     * Clear all caches
     */
    public void clearAll() {
        cache.clear();
    }
    
    /**
     * Get cache size for a database
     */
    public int size(String dbName) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        return dbCache != null ? dbCache.size() : 0;
    }
    
    /**
     * Get total size across all databases
     */
    public int totalSize() {
        return cache.values().stream().mapToInt(Map::size).sum();
    }
    
    /**
     * Get all database names in cache
     */
    public Set<String> getDatabaseNames() {
        return cache.keySet();
    }
    
    /**
     * Map ResultSet to entity using reflection
     */
    private T mapResultSetToEntity(ResultSet rs) throws SQLException {
        try {
            T entity = entityClass.getDeclaredConstructor().newInstance();
            
            for (Map.Entry<String, Field> entry : columnFieldMap.entrySet()) {
                String columnName = entry.getKey();
                Field field = entry.getValue();
                Object value = rs.getObject(columnName);
                
                if (value != null) {
                    // Handle type conversions
                    if (field.getType() == Long.class || field.getType() == long.class) {
                        value = rs.getLong(columnName);
                    } else if (field.getType() == Integer.class || field.getType() == int.class) {
                        value = rs.getInt(columnName);
                    } else if (field.getType() == Boolean.class || field.getType() == boolean.class) {
                        value = rs.getBoolean(columnName);
                    } else if (field.getType() == String.class) {
                        value = rs.getString(columnName);
                    }
                    
                    field.set(entity, value);
                }
            }
            
            return entity;
        } catch (Exception e) {
            throw new SQLException("Failed to map ResultSet to entity", e);
        }
    }
    
    /**
     * Create a WAL entry for an entity (protected for subclass use)
     */
    protected WALEntry createWALEntry(String dbName, T entity, WALEntry.OperationType operationType) {
        try {
            Map<String, Object> data = new HashMap<>();
            
            for (Map.Entry<String, Field> entry : columnFieldMap.entrySet()) {
                String columnName = entry.getKey();
                Field field = entry.getValue();
                Object value = field.get(entity);
                if (value != null) {
                    data.put(columnName, value);
                }
            }
            
            return WALEntry.builder()
                .dbName(dbName)
                .tableName(tableName)
                .operationType(operationType)
                .data(data)
                .build();
        } catch (Exception e) {
            logger.error("Failed to create WAL entry", e);
            return null;
        }
    }
    
    /**
     * Write entity changes to WAL (writes as batch of size 1)
     */
    private void writeToWAL(String dbName, T entity, WALEntry.OperationType operationType) {
        WALEntry walEntry = createWALEntry(dbName, entity, operationType);
        if (walEntry != null && walWriter != null) {
            walWriter.write(walEntry); // This will call write(List.of(entry))
        }
    }
    
    // Getters
    public String getTableName() {
        return tableName;
    }
    
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
    
    public Class<T> getEntityClass() {
        return entityClass;
    }
}