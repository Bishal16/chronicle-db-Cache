package com.telcobright.oltp.service;

import com.telcobright.oltp.dbCache.CacheManager;
import com.telcobright.oltp.queue.chronicle.ConsumerToQueue;
import com.telcobright.core.wal.WALEntry;
import com.zaxxer.hikari.HikariDataSource;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireIn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Universal consumer that reads batches from WAL and writes them to database transactionally.
 * ALL entries are processed as batches (even single entries are batches of size 1).
 */
public class UniversalConsumer implements Runnable, ConsumerToQueue<Object> {
    private static final Logger logger = LoggerFactory.getLogger(UniversalConsumer.class);
    
    private final ChronicleQueue queue;
    private final String consumerName;
    private final HikariDataSource dataSource;
    private final String offsetTable;
    private final boolean replayOnStart;
    private final ExcerptTailer tailer;
    private final ExcerptAppender appender;
    private final PendingStatusChecker pendingStatusChecker;
    private final CacheManager cacheManager;
    
    private volatile boolean running = true;
    
    public UniversalConsumer(ChronicleQueue queue, ExcerptAppender appender, String consumerName,
                           HikariDataSource dataSource, String offsetTable, boolean replayOnStart, 
                           PendingStatusChecker pendingStatusChecker, CacheManager cacheManager) {
        this.queue = queue;
        this.appender = appender;
        this.consumerName = consumerName;
        this.dataSource = dataSource;
        this.offsetTable = offsetTable;
        this.replayOnStart = replayOnStart;
        this.pendingStatusChecker = pendingStatusChecker;
        this.cacheManager = cacheManager;
        this.tailer = queue.createTailer(consumerName);
        
        logger.info("✅ Initialized universal consumer '{}' for queue", consumerName);
        
        setTailerToLastProcessedOffset();
        
        try {
            if (replayOnStart && hasPendingMessages()) {
                logger.info("▶️ Starting pending msg replay with consumer {}", consumerName);
                replayPendingMessages();
                logger.info("✅ Done with replay pending msg {}", consumerName);
            }
        } catch (Exception e) {
            logger.error("Error checking pending messages: ", e);
            throw new RuntimeException(e);
        }
        pendingStatusChecker.markReplayComplete();
    }
    
    @Override
    public void run() {
        logger.info("▶️ Consumer '{}' started", consumerName);
        processLiveMessages();
    }
    
    /**
     * Process live messages - reading batches and writing to DB transactionally
     */
    private void processLiveMessages() {
        while (running) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.isPresent()) {
                    processBatchTransaction(dc);
                } else {
                    // No new messages, sleep briefly
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Consumer interrupted");
                break;
            } catch (Exception e) {
                logger.error("Error processing batch", e);
            }
        }
    }
    
    /**
     * Process a batch transaction from WAL
     */
    private void processBatchTransaction(DocumentContext dc) {
        Wire wire = dc.wire();
        long offset = dc.index();
        
        // Read batch header
        String transactionId = wire.read("transactionId").text();
        int batchSize = wire.read("batchSize").int32();
        long timestamp = wire.read("timestamp").int64();
        
        logger.debug("Processing batch transaction: txId={}, size={}, offset={}", 
            transactionId, batchSize, offset);
        
        // Read all entries in the batch
        List<WALEntry> entries = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            final int idx = i;
            wire.read("entry_" + i).marshallable(m -> {
                WALEntry entry = readWALEntry(m);
                if (entry != null) {
                    entry.setTransactionId(transactionId);
                    entries.add(entry);
                }
            });
        }
        
        // Process the batch transactionally
        if (!entries.isEmpty()) {
            processBatchToDatabase(entries, offset, transactionId);
        }
    }
    
    /**
     * Read a single WAL entry from wire
     */
    private WALEntry readWALEntry(WireIn wire) {
        try {
            String dbName = wire.read("dbName").text();
            String tableName = wire.read("tableName").text();
            String operationType = wire.read("operationType").text();
            
            // Read data map
            Map<String, Object> data = new HashMap<>();
            int dataSize = wire.read("dataSize").int32();
            
            for (int i = 0; i < dataSize; i++) {
                String key = wire.read("key").text();
                String valueType = wire.read("valueType").text();
                
                Object value = null;
                switch (valueType) {
                    case "NULL":
                        value = null;
                        break;
                    case "STRING":
                        value = wire.read("value").text();
                        break;
                    case "LONG":
                        value = wire.read("value").int64();
                        break;
                    case "INT":
                        value = wire.read("value").int32();
                        break;
                    case "DOUBLE":
                        value = wire.read("value").float64();
                        break;
                    case "BOOLEAN":
                        value = wire.read("value").bool();
                        break;
                    default:
                        value = wire.read("value").text();
                        break;
                }
                data.put(key, value);
            }
            
            return WALEntry.builder()
                .dbName(dbName)
                .tableName(tableName)
                .operationType(WALEntry.OperationType.valueOf(operationType))
                .data(data)
                .build();
                
        } catch (Exception e) {
            logger.error("Error reading WAL entry", e);
            return null;
        }
    }
    
    /**
     * Process batch to database with full transaction support
     */
    private void processBatchToDatabase(List<WALEntry> entries, long offset, String transactionId) {
        Connection conn = null;
        boolean originalAutoCommit = true;
        
        try {
            conn = dataSource.getConnection();
            originalAutoCommit = conn.getAutoCommit();
            
            // Start transaction
            conn.setAutoCommit(false);
            
            // Process each entry in the batch
            int processedCount = 0;
            for (WALEntry entry : entries) {
                boolean success = processEntryToDatabase(conn, entry);
                if (!success) {
                    throw new SQLException("Failed to process entry for " + 
                        entry.getDbName() + "." + entry.getTableName());
                }
                processedCount++;
            }
            
            // Save offset as part of the transaction
            saveOffset(conn, offset);
            
            // Commit the entire batch
            conn.commit();
            
            logger.info("✅ Committed batch transaction: txId={}, entries={}, offset={}", 
                transactionId, processedCount, offset);
            
        } catch (Exception e) {
            // Rollback on any error
            if (conn != null) {
                try {
                    conn.rollback();
                    logger.error("❌ Rolled back batch transaction: txId={}, error={}", 
                        transactionId, e.getMessage());
                } catch (SQLException rollbackEx) {
                    logger.error("Failed to rollback transaction", rollbackEx);
                }
            }
            logger.error("Error processing batch to database", e);
            
        } finally {
            // Restore original auto-commit setting
            if (conn != null) {
                try {
                    conn.setAutoCommit(originalAutoCommit);
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Error closing connection", e);
                }
            }
        }
    }
    
    /**
     * Process a single entry to database (within a transaction)
     */
    private boolean processEntryToDatabase(Connection conn, WALEntry entry) {
        try {
            // Get pre-built SQL from cache
            SqlStatementCache.EntitySqlStatements sqlStatements = 
                SqlStatementCache.get(getEntityClass(entry.getTableName()));
            
            if (sqlStatements == null) {
                logger.error("No SQL statements found for table: {}", entry.getTableName());
                return false;
            }
            
            String sql = null;
            switch (entry.getOperationType()) {
                case INSERT:
                case UPSERT:
                    sql = SqlStatementCache.getInsertSql(
                        getEntityClass(entry.getTableName()), 
                        entry.getDbName()
                    );
                    return executeInsert(conn, sql, entry, sqlStatements);
                    
                case UPDATE:
                    sql = SqlStatementCache.getUpdateSql(
                        getEntityClass(entry.getTableName()), 
                        entry.getDbName()
                    );
                    return executeUpdate(conn, sql, entry, sqlStatements);
                    
                case DELETE:
                    sql = SqlStatementCache.getDeleteSql(
                        getEntityClass(entry.getTableName()), 
                        entry.getDbName()
                    );
                    return executeDelete(conn, sql, entry, sqlStatements);
                    
                default:
                    logger.warn("Unsupported operation type: {}", entry.getOperationType());
                    return false;
            }
            
        } catch (Exception e) {
            logger.error("Error processing entry to database", e);
            return false;
        }
    }
    
    private boolean executeInsert(Connection conn, String sql, WALEntry entry, 
                                 SqlStatementCache.EntitySqlStatements sqlStatements) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int paramIndex = 1;
            for (String columnName : sqlStatements.columnNames) {
                Object value = entry.getData().get(columnName);
                stmt.setObject(paramIndex++, value);
            }
            
            int affected = stmt.executeUpdate();
            return affected > 0;
        }
    }
    
    private boolean executeUpdate(Connection conn, String sql, WALEntry entry,
                                 SqlStatementCache.EntitySqlStatements sqlStatements) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int paramIndex = 1;
            
            // Set non-primary key columns
            for (String columnName : sqlStatements.columnNames) {
                if (!columnName.equals(sqlStatements.primaryKeyColumn)) {
                    Object value = entry.getData().get(columnName);
                    stmt.setObject(paramIndex++, value);
                }
            }
            
            // Set primary key for WHERE clause
            Object primaryKeyValue = entry.getData().get(sqlStatements.primaryKeyColumn);
            stmt.setObject(paramIndex, primaryKeyValue);
            
            int affected = stmt.executeUpdate();
            return affected > 0;
        }
    }
    
    private boolean executeDelete(Connection conn, String sql, WALEntry entry,
                                 SqlStatementCache.EntitySqlStatements sqlStatements) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            Object primaryKeyValue = entry.getData().get(sqlStatements.primaryKeyColumn);
            stmt.setObject(1, primaryKeyValue);
            
            int affected = stmt.executeUpdate();
            return affected > 0;
        }
    }
    
    /**
     * Get entity class from table name
     */
    private Class<?> getEntityClass(String tableName) {
        // Map table names to entity classes
        switch (tableName.toLowerCase()) {
            case "packageaccount":
                return com.telcobright.oltp.entity.PackageAccount.class;
            case "packageaccountreserve":
                return com.telcobright.oltp.entity.PackageAccountReserve.class;
            default:
                logger.warn("Unknown table name: {}", tableName);
                return null;
        }
    }
    
    /**
     * Save the last processed offset
     */
    private void saveOffset(Connection conn, long offset) throws SQLException {
        String dbName = loadDbNameFromProperties();
        String sql = "INSERT INTO " + dbName + "." + offsetTable + 
            " (consumer_id, last_offset) VALUES (?, ?) " +
            "ON DUPLICATE KEY UPDATE last_offset = ?";
            
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, consumerName);
            stmt.setLong(2, offset);
            stmt.setLong(3, offset);
            stmt.executeUpdate();
        }
    }
    
    /**
     * Get the last processed offset from database
     */
    private long getLastOffsetFromDb(Connection conn) throws SQLException {
        String dbName = loadDbNameFromProperties();
        String sql = "SELECT last_offset FROM " + dbName + "." + offsetTable + 
            " WHERE consumer_id = ?";
            
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, consumerName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("last_offset");
                }
            }
        }
        return -1; // No offset found
    }
    
    private void setTailerToLastProcessedOffset() {
        try (Connection conn = dataSource.getConnection()) {
            String dbName = loadDbNameFromProperties();
            
            // Create offset table if not exists
            try (PreparedStatement createStmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS " + dbName + "." + offsetTable + " (" +
                            "consumer_id VARCHAR(255) PRIMARY KEY, " +
                            "last_offset BIGINT NOT NULL" +
                            ")")) {
                createStmt.executeUpdate();
            }
            
            // Get last offset
            long lastSavedOffset = getLastOffsetFromDb(conn);
            if (lastSavedOffset >= 0) {
                logger.info("Last saved offset for consumer '{}': {}", consumerName, lastSavedOffset);
                tailer.moveToIndex(lastSavedOffset + 1);
            } else {
                logger.info("No previous offset for consumer '{}'. Starting fresh.", consumerName);
            }
            
        } catch (SQLException e) {
            logger.error("Failed to load offset for consumer '{}': {}", consumerName, e.getMessage());
            throw new RuntimeException("Offset load failed", e);
        }
    }
    
    private boolean hasPendingMessages() {
        long currentIndex = tailer.index();
        // Move to end and get that index
        ExcerptTailer tempTailer = queue.createTailer();
        tempTailer.toEnd();
        long lastIndex = tempTailer.index();
        return currentIndex < lastIndex;
    }
    
    private void replayPendingMessages() {
        long startIndex = tailer.index();
        // Get the last index by creating a temporary tailer
        ExcerptTailer tempTailer = queue.createTailer();
        tempTailer.toEnd();
        long lastIndex = tempTailer.index();
        long processed = 0;
        
        logger.info("Replaying messages from index {} to {}", startIndex, lastIndex);
        
        while (tailer.index() <= lastIndex && running) {
            try (DocumentContext dc = tailer.readingDocument()) {
                if (dc.isPresent()) {
                    processBatchTransaction(dc);
                    processed++;
                    
                    if (processed % 100 == 0) {
                        logger.info("Replayed {} batches...", processed);
                    }
                }
            }
        }
        
        logger.info("✅ Replay complete. Processed {} batches", processed);
    }
    
    private String loadDbNameFromProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
                return props.getProperty("db.name", "admin");
            }
        } catch (IOException e) {
            logger.error("Error loading properties", e);
        }
        return "admin";
    }
    
    public void stop() {
        running = false;
    }
    
    @Override
    public void shutdown() {
        stop();
        logger.info("Consumer '{}' shutdown", consumerName);
    }
    
    @Override
    public void updatePackageAccountTable(Object message, Connection connection) throws SQLException {
        // Not used in batch processing mode - we process batches directly from queue
    }
}