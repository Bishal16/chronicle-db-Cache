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
     * Load all data from a specific database
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
     * Get an entity by key from a specific database
     */
    public T get(String dbName, K key) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        return dbCache != null ? dbCache.get(key) : null;
    }
    
    /**
     * Put an entity into cache for a specific database
     */
    public void put(String dbName, T entity) {
        K key = extractKey(entity);
        cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>()).put(key, entity);
        
        // Write to WAL if available
        if (walWriter != null) {
            writeToWAL(dbName, entity, WALEntry.OperationType.UPSERT);
        }
    }
    
    /**
     * Update an entity in cache
     */
    public void update(String dbName, K key, T entity) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        if (dbCache != null) {
            dbCache.put(key, entity);
            
            // Write to WAL if available
            if (walWriter != null) {
                writeToWAL(dbName, entity, WALEntry.OperationType.UPDATE);
            }
        }
    }
    
    /**
     * Delete an entity from cache
     */
    public void delete(String dbName, K key) {
        ConcurrentHashMap<K, T> dbCache = cache.get(dbName);
        if (dbCache != null) {
            T removed = dbCache.remove(key);
            
            // Write to WAL if available
            if (removed != null && walWriter != null) {
                writeToWAL(dbName, removed, WALEntry.OperationType.DELETE);
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
     * Write entity changes to WAL
     */
    private void writeToWAL(String dbName, T entity, WALEntry.OperationType operationType) {
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
            
            WALEntry walEntry = WALEntry.builder()
                .dbName(dbName)
                .tableName(tableName)
                .operationType(operationType)
                .data(data)
                .build();
            
            walWriter.write(walEntry);
        } catch (Exception e) {
            logger.error("Failed to write to WAL", e);
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