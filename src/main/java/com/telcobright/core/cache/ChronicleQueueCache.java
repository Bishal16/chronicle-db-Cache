package com.telcobright.core.cache;

import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALWriter;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generic base class for Chronicle Queue-based caching with WAL support.
 * This is a core library component that can be extended for any entity type.
 * 
 * @param <TEntity> The entity type that extends CacheableEntity
 * @param <TDelta> The delta operation type that extends DeltaOperation
 */
public abstract class ChronicleQueueCache<TEntity extends CacheableEntity, TDelta extends DeltaOperation> {
    
    private static final Logger logger = LoggerFactory.getLogger(ChronicleQueueCache.class);
    
    // Multi-tenant cache: dbName -> (entityId -> entity)
    protected final ConcurrentHashMap<String, ConcurrentHashMap<Long, TEntity>> cache;
    
    // Core components
    protected DataSource dataSource;
    protected WALWriter walWriter;
    protected String adminDb;
    protected final String entityName;
    
    /**
     * Create a new cache instance for the specified entity type.
     * @param entityName The name of the entity type being cached
     */
    protected ChronicleQueueCache(String entityName) {
        this.entityName = entityName;
        this.cache = new ConcurrentHashMap<>();
    }
    
    /**
     * Initialize the cache with a DataSource and WALWriter.
     * @param dataSource The database connection pool
     * @param walWriter The WAL writer for persistence
     * @param adminDb The admin database name
     */
    public void initialize(DataSource dataSource, WALWriter walWriter, String adminDb) {
        this.dataSource = dataSource;
        this.walWriter = walWriter;
        this.adminDb = adminDb;
    }
    
    /**
     * Initialize the cache from the database.
     * Discovers all databases and loads entities from each.
     */
    public void initFromDb() throws SQLException {
        List<String> dbNames = discoverDatabases();
        
        for (String dbName : dbNames) {
            ConcurrentHashMap<Long, TEntity> dbCache = new ConcurrentHashMap<>();
            
            try (Connection conn = dataSource.getConnection()) {
                loadEntitiesFromDb(conn, dbName, dbCache);
                cache.put(dbName, dbCache);
                logger.info("✅ {} cache initialized for db={}, count={}", 
                    entityName, dbName, dbCache.size());
            } catch (SQLException e) {
                logger.error("❌ Failed to load cache for database: {}", dbName, e);
            }
        }
    }
    
    /**
     * Discover all databases to cache.
     * Can be overridden for custom discovery logic.
     */
    protected List<String> discoverDatabases() throws SQLException {
        List<String> databases = new ArrayList<>();
        
        // Default implementation: get from admin database
        String sql = String.format("""
            SELECT DISTINCT accountDbName 
            FROM %s.reselleraccountdb 
            WHERE accountDbName IS NOT NULL
        """, adminDb);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                databases.add(rs.getString("accountDbName"));
            }
        }
        
        if (databases.isEmpty()) {
            databases.add(adminDb); // Fallback to admin db
        }
        
        return databases;
    }
    
    /**
     * Apply delta operations to cache and write to WAL.
     * @param deltas List of delta operations to apply
     */
    public void applyDeltasAndWriteToWAL(List<TDelta> deltas) {
        if (deltas == null || deltas.isEmpty()) {
            return;
        }
        
        // Apply to cache first
        List<WALEntry> walEntries = new ArrayList<>();
        String transactionId = UUID.randomUUID().toString();
        
        for (TDelta delta : deltas) {
            TEntity entity = applyDeltaToCache(delta);
            
            if (entity != null) {
                // Create WAL entry
                WALEntry entry = createWALEntryForDelta(delta, entity, transactionId);
                walEntries.add(entry);
                
                // Log the application
                logDeltaApplication(delta, entity);
            }
        }
        
        // Write to WAL
        if (!walEntries.isEmpty()) {
            // Set transaction ID for all entries
            for (WALEntry entry : walEntries) {
                entry.setTransactionId(transactionId);
            }
            walWriter.write(walEntries);
            
            logger.info("✅ Wrote {} delta(s) to WAL for {}", walEntries.size(), entityName);
        }
    }
    
    /**
     * Delete an entity from cache and write to WAL.
     * @param dbName The database name
     * @param entityId The entity ID to delete
     */
    public void deleteAndWriteToWAL(String dbName, Long entityId) {
        ConcurrentHashMap<Long, TEntity> dbCache = cache.get(dbName);
        
        if (dbCache != null && dbCache.containsKey(entityId)) {
            TEntity removed = dbCache.remove(entityId);
            
            // Create delete WAL entry
            WALEntry entry = new WALEntry.Builder(dbName, getTableName(), WALEntry.OperationType.DELETE)
                .withData("where_" + getPrimaryKeyColumn(), entityId)
                .build();
            
            walWriter.write(entry);
            
            logger.info("✅ Deleted {} {} from cache and wrote to WAL", entityName, entityId);
        } else {
            logger.warn("⚠️ {} {} not found in cache for deletion", entityName, entityId);
        }
    }
    
    /**
     * Insert a new entity to cache and write to WAL.
     * @param dbName The database name
     * @param entity The entity to insert
     */
    public void insertAndWriteToWAL(String dbName, TEntity entity) {
        ConcurrentHashMap<Long, TEntity> dbCache = cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>());
        
        dbCache.put(entity.getId(), entity);
        
        // Create insert WAL entry
        WALEntry entry = createWALEntryForInsert(dbName, entity);
        walWriter.write(entry);
        
        logger.info("✅ Inserted {} {} to cache and wrote to WAL", entityName, entity.getId());
    }
    
    /**
     * Apply a delta to the cache.
     * @param delta The delta operation to apply
     * @return The updated entity, or null if not found
     */
    protected TEntity applyDeltaToCache(TDelta delta) {
        ConcurrentHashMap<Long, TEntity> dbCache = cache.get(delta.getDbName());
        
        if (dbCache == null) {
            logger.warn("⚠️ No cache found for database: {}", delta.getDbName());
            return null;
        }
        
        TEntity entity = dbCache.get(delta.getAccountId());
        
        if (entity == null) {
            logger.warn("⚠️ {} {} not found in cache", entityName, delta.getAccountId());
            return null;
        }
        
        // Apply delta
        synchronized (entity) {
            entity.applyDelta(delta.getAmount());
        }
        
        return entity;
    }
    
    /**
     * Get the cache for all databases.
     * @return The multi-tenant cache map
     */
    public ConcurrentHashMap<String, ConcurrentHashMap<Long, TEntity>> getCache() {
        return cache;
    }
    
    /**
     * Get an entity from cache.
     * @param dbName The database name
     * @param entityId The entity ID
     * @return The entity, or null if not found
     */
    public TEntity getEntity(String dbName, Long entityId) {
        ConcurrentHashMap<Long, TEntity> dbCache = cache.get(dbName);
        return dbCache != null ? dbCache.get(entityId) : null;
    }
    
    /**
     * Update an entity in the cache.
     * @param dbName The database name
     * @param entity The entity to update
     */
    public void updateEntityInCache(String dbName, TEntity entity) {
        if (dbName == null || entity == null || entity.getId() == null) {
            throw new IllegalArgumentException("dbName, entity, and entity.id must not be null");
        }
        
        cache.computeIfAbsent(dbName, k -> new ConcurrentHashMap<>())
             .put(entity.getId(), entity);
        
        logger.info("✅ Updated {} cache for DB={}, id={}", entityName, dbName, entity.getId());
    }
    
    // Abstract methods to be implemented by subclasses
    
    /**
     * Load entities from the database into the cache.
     * @param conn The database connection
     * @param dbName The database name
     * @param cache The cache to populate
     */
    protected abstract void loadEntitiesFromDb(Connection conn, String dbName,
                                              ConcurrentHashMap<Long, TEntity> cache) throws SQLException;
    
    /**
     * Map a ResultSet row to an entity.
     * @param rs The ResultSet
     * @return The mapped entity
     */
    protected abstract TEntity mapResultSetToEntity(ResultSet rs) throws SQLException;
    
    /**
     * Get the database table name for this entity type.
     * @return The table name
     */
    protected abstract String getTableName();
    
    /**
     * Get the primary key column name.
     * @return The primary key column name
     */
    protected abstract String getPrimaryKeyColumn();
    
    /**
     * Create a WAL entry for a delta operation.
     * @param delta The delta operation
     * @param entity The entity after applying the delta
     * @param transactionId The transaction ID
     * @return The WAL entry
     */
    protected abstract WALEntry createWALEntryForDelta(TDelta delta, TEntity entity, String transactionId);
    
    /**
     * Create a WAL entry for an insert operation.
     * @param dbName The database name
     * @param entity The entity to insert
     * @return The WAL entry
     */
    protected abstract WALEntry createWALEntryForInsert(String dbName, TEntity entity);
    
    /**
     * Log the application of a delta.
     * @param delta The delta that was applied
     * @param entity The entity after applying the delta
     */
    protected abstract void logDeltaApplication(TDelta delta, TEntity entity);
}