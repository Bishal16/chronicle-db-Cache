package com.telcobright.oltp.dbCache;

import com.telcobright.oltp.entity.CacheableEntity;
import com.telcobright.oltp.entity.DeltaOperation;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.zaxxer.hikari.HikariDataSource;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public abstract class ChronicleQueueCache<TEntity extends CacheableEntity, TDelta extends DeltaOperation> 
        extends JdbcCache<Long, TEntity, List<TDelta>> {
    
    protected ChronicleInstance chronicleInstance;
    protected String adminDb;
    protected String entityName;
    
    private static final Logger logger = LoggerFactory.getLogger(ChronicleQueueCache.class);
    
    protected ChronicleQueueCache(String entityName) {
        super();
        this.entityName = entityName;
    }
    
    public void setChronicleInstance(ChronicleInstance chronicleInstance) {
        this.chronicleInstance = chronicleInstance;
    }
    
    public void setAdminDb(String adminDb) {
        this.adminDb = adminDb;
    }
    
    protected List<String> getAllResellerDbNames() {
        List<String> dbNames = new ArrayList<>();
        
        String sql = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'res\\_%'
        """;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                dbNames.add(rs.getString("schema_name"));
            }
        } catch (SQLException e) {
            logger.error("Failed to get reseller database names", e);
        }
        return dbNames;
    }
    
    @Override
    public void initFromDb() throws SQLException {
        List<String> dbNames = getAllResellerDbNames();
        dbNames.add(adminDb);
        
        try (Connection conn = getConnection()) {
            for (String dbName : dbNames) {
                ConcurrentHashMap<Long, TEntity> entityCache = new ConcurrentHashMap<>();
                loadEntitiesFromDb(conn, dbName, entityCache);
                dbVsPkgIdVsPkgAccountCache.put(dbName, entityCache);
                logger.info("✅ {} cache initialized for db={}, count={}", 
                    entityName, dbName, entityCache.size());
            }
        }
    }
    
    protected abstract void loadEntitiesFromDb(Connection conn, String dbName, 
        ConcurrentHashMap<Long, TEntity> cache) throws SQLException;
    
    protected abstract TEntity mapResultSetToEntity(ResultSet rs) throws SQLException;
    
    @Override
    protected void writeWALForUpdate(List<TDelta> deltas) {
        ExcerptAppender appender = chronicleInstance.getAppender();
        appender.writeDocument(w -> {
            w.write("entity").text(entityName);
            w.write("action").int32(CrudActionType.Update.ordinal());
            w.write("size").int32(deltas.size());
            for (TDelta delta : deltas) {
                writeDeltaToWAL(w, delta);
            }
        });
        long lastIndex = appender.lastIndexAppended();
        logger.info("Wrote {} UPDATE WAL entry at index: {}", entityName, lastIndex);
    }
    
    protected abstract void writeDeltaToWAL(WireOut wire, TDelta delta);
    
    @Override
    protected Consumer<List<TDelta>> updateCache() {
        return deltas -> {
            for (TDelta delta : deltas) {
                ConcurrentHashMap<Long, TEntity> dbCache = dbVsPkgIdVsPkgAccountCache.get(delta.getDbName());
                if (dbCache == null) {
                    throw new RuntimeException("Database cache not found: " + delta.getDbName());
                }
                
                TEntity entity = dbCache.get(delta.getAccountId());
                if (entity == null) {
                    throw new RuntimeException(entityName + " [id: " + delta.getAccountId() + 
                        "] not found in cache for db: " + delta.getDbName());
                }
                
                entity.applyDelta(delta.getAmount());
                logDeltaApplication(delta, entity);
            }
        };
    }
    
    protected abstract void logDeltaApplication(TDelta delta, TEntity entity);
    
    @Override
    protected void writeWALForDelete(String dbName, Long entityId) {
        ExcerptAppender appender = chronicleInstance.getAppender();
        appender.writeDocument(w -> {
            w.write("entity").text(entityName);
            w.write("action").int32(CrudActionType.Delete.ordinal());
            w.write("dbName").text(dbName);
            w.write("entityId").int64(entityId);
        });
        long lastIndex = appender.lastIndexAppended();
        logger.info("Wrote {} DELETE WAL entry at index: {} for id: {} in db: {}", 
            entityName, lastIndex, entityId, dbName);
    }
    
    @Override
    protected Consumer<String> deleteFromCache(Long entityId) {
        return dbName -> {
            ConcurrentHashMap<Long, TEntity> dbCache = dbVsPkgIdVsPkgAccountCache.get(dbName);
            if (dbCache != null) {
                TEntity removed = dbCache.remove(entityId);
                if (removed != null) {
                    logger.info("✅ Deleted {} from cache: id={}, dbName={}", entityName, entityId, dbName);
                } else {
                    logger.warn("⚠️ {} not found in cache for deletion: id={}, dbName={}", 
                        entityName, entityId, dbName);
                }
            } else {
                logger.warn("⚠️ Database cache not found for deletion: dbName={}", dbName);
            }
        };
    }
    
    public ConcurrentHashMap<String, ConcurrentHashMap<Long, TEntity>> getCache() {
        return this.dbVsPkgIdVsPkgAccountCache;
    }
    
    public void updateEntityInCache(String dbName, TEntity entity) {
        if (dbName == null || entity == null || entity.getId() == null) {
            throw new IllegalArgumentException("dbName, entity, and entity.id must not be null");
        }
        
        dbVsPkgIdVsPkgAccountCache
            .computeIfAbsent(dbName, k -> new ConcurrentHashMap<>())
            .put(entity.getId(), entity);
        
        logger.info("✅ Updated {} cache for DB={}, id={}", entityName, dbName, entity.getId());
    }
}