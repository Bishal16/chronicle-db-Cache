package com.telcobright.core.consumer;

import com.telcobright.core.sql.SQLOperationHandler;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Generic transactional consumer that processes WAL entries from Chronicle Queue
 * and applies them to a MySQL database with proper transaction management.
 */
public class TransactionalConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TransactionalConsumer.class);
    
    private final ChronicleQueue queue;
    private final DataSource dataSource;
    private final String consumerName;
    private final String offsetTable;
    private final String offsetDb;
    
    private final ExcerptTailer tailer;
    private final WALReader walReader;
    private final SQLOperationHandler sqlHandler;
    private final AtomicBoolean running;
    
    private ConsumerListener listener;
    
    public interface ConsumerListener {
        void beforeProcess(WALEntry entry);
        void afterProcess(WALEntry entry, boolean success, Exception error);
        void onBatchComplete(List<WALEntry> entries, boolean success);
    }
    
    public TransactionalConsumer(ChronicleQueue queue, DataSource dataSource, 
                                String consumerName, String offsetTable, String offsetDb) {
        this.queue = queue;
        this.dataSource = dataSource;
        this.consumerName = consumerName;
        this.offsetTable = offsetTable;
        this.offsetDb = offsetDb;
        
        this.tailer = queue.createTailer(consumerName);
        this.walReader = new WALReader();
        this.sqlHandler = new SQLOperationHandler();
        this.running = new AtomicBoolean(true);
        
        initializeOffset();
        logger.info("✅ Initialized TransactionalConsumer '{}' for queue", consumerName);
    }
    
    public void setListener(ConsumerListener listener) {
        this.listener = listener;
    }
    
    private void initializeOffset() {
        try (Connection conn = dataSource.getConnection()) {
            // Create offset table if not exists
            String createTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s.%s (" +
                "consumer_id VARCHAR(255) PRIMARY KEY, " +
                "last_offset BIGINT NOT NULL, " +
                "last_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                ")", offsetDb, offsetTable);
            
            try (PreparedStatement stmt = conn.prepareStatement(createTable)) {
                stmt.executeUpdate();
            }
            
            // Load last offset
            long lastOffset = getLastOffset(conn);
            if (lastOffset >= 0) {
                tailer.moveToIndex(lastOffset + 1);
                logger.info("Resumed from offset: {} for consumer '{}'", lastOffset, consumerName);
            }
        } catch (SQLException e) {
            logger.error("Failed to initialize offset for consumer '{}'", consumerName, e);
            throw new RuntimeException("Failed to initialize consumer offset", e);
        }
    }
    
    @Override
    public void run() {
        logger.info("▶️ TransactionalConsumer '{}' started", consumerName);
        
        while (running.get()) {
            try {
                processNextEntry();
            } catch (Exception e) {
                logger.error("Error processing entry in consumer '{}'", consumerName, e);
                // Optional: implement backoff strategy
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        logger.info("⏹️ TransactionalConsumer '{}' stopped", consumerName);
    }
    
    private void processNextEntry() throws SQLException {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                // No new messages, wait a bit
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running.set(false);
                }
                return;
            }
            
            // Read WAL entry
            WALEntry entry = walReader.readEntry(dc.wire());
            long currentOffset = tailer.index();
            
            // Process based on operation type
            if (entry.getOperationType() == WALEntry.OperationType.BATCH) {
                processBatch(entry, currentOffset);
            } else {
                processSingleEntry(entry, currentOffset);
            }
        }
    }
    
    private void processSingleEntry(WALEntry entry, long offset) throws SQLException {
        Connection conn = null;
        boolean success = false;
        Exception error = null;
        
        try {
            conn = dataSource.getConnection();
            
            // Start transaction
            conn.setAutoCommit(false);
            
            // Notify listener before processing
            if (listener != null) {
                listener.beforeProcess(entry);
            }
            
            // Execute the SQL operation
            int affected = sqlHandler.execute(conn, entry);
            
            // Save offset
            saveOffset(conn, offset);
            
            // Commit transaction
            conn.commit();
            success = true;
            
            logger.debug("✅ Processed entry: {} rows affected for {}.{} [{}]", 
                affected, entry.getDbName(), entry.getTableName(), entry.getOperationType());
            
        } catch (SQLException e) {
            error = e;
            if (conn != null) {
                try {
                    conn.rollback();
                    logger.error("❌ Rolled back transaction for offset: {}", offset, e);
                } catch (SQLException re) {
                    logger.error("Failed to rollback transaction", re);
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection", e);
                }
            }
            
            // Notify listener after processing
            if (listener != null) {
                listener.afterProcess(entry, success, error);
            }
        }
    }
    
    private void processBatch(WALEntry batchHeader, long offset) throws SQLException {
        Connection conn = null;
        List<WALEntry> entries = null;
        boolean success = false;
        
        try {
            conn = dataSource.getConnection();
            
            // Start transaction for entire batch
            conn.setAutoCommit(false);
            
            // Read batch entries
            entries = walReader.readBatch((net.openhft.chronicle.wire.Wire) batchHeader.get("_wire"));
            
            // Process each entry in the batch
            for (WALEntry entry : entries) {
                if (listener != null) {
                    listener.beforeProcess(entry);
                }
                
                int affected = sqlHandler.execute(conn, entry);
                
                logger.debug("Batch item processed: {} rows affected for {}.{}", 
                    affected, entry.getDbName(), entry.getTableName());
            }
            
            // Save offset after all batch items processed
            saveOffset(conn, offset);
            
            // Commit entire batch
            conn.commit();
            success = true;
            
            logger.info("✅ Batch processed: {} entries, txId: {}", 
                entries.size(), batchHeader.getTransactionId());
            
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    logger.error("❌ Rolled back batch transaction for offset: {}", offset, e);
                } catch (SQLException re) {
                    logger.error("Failed to rollback batch transaction", re);
                }
            }
            throw e;
        } finally {
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Failed to close connection", e);
                }
            }
            
            // Notify listener about batch completion
            if (listener != null && entries != null) {
                listener.onBatchComplete(entries, success);
            }
        }
    }
    
    private long getLastOffset(Connection conn) throws SQLException {
        String sql = String.format(
            "SELECT last_offset FROM %s.%s WHERE consumer_id = ?",
            offsetDb, offsetTable);
        
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
    
    private void saveOffset(Connection conn, long offset) throws SQLException {
        String sql = String.format(
            "INSERT INTO %s.%s (consumer_id, last_offset) VALUES (?, ?) " +
            "ON DUPLICATE KEY UPDATE last_offset = ?, last_processed = CURRENT_TIMESTAMP",
            offsetDb, offsetTable);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, consumerName);
            stmt.setLong(2, offset);
            stmt.setLong(3, offset);
            stmt.executeUpdate();
        }
    }
    
    public void stop() {
        running.set(false);
        logger.info("Stopping consumer '{}'", consumerName);
    }
    
    public boolean isRunning() {
        return running.get();
    }
}