package com.telcobright.core.cache.wal.impl;

import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALReader;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Chronicle Queue based implementation of WALConsumer.
 * Manages offset tracking in database for transactional consistency.
 */
public class ChronicleConsumer implements WALConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleConsumer.class);
    
    private final ChronicleQueue queue;
    private final ExcerptTailer tailer;
    private final WALReader reader;
    private final Connection offsetConnection;
    private final String consumerName;
    private final String offsetTable;
    private volatile boolean healthy = true;
    private long currentOffset = -1;
    private int batchSize = 100; // Default batch size
    
    public ChronicleConsumer(ChronicleQueue queue, Connection offsetConnection, 
                           String consumerName, String offsetTable) {
        this.queue = queue;
        this.tailer = queue.createTailer();
        this.reader = new WALReader();
        this.offsetConnection = offsetConnection;
        this.consumerName = consumerName;
        this.offsetTable = offsetTable;
        
        try {
            initializeOffsetTable();
            long lastOffset = getLastCommittedOffset();
            if (lastOffset >= 0) {
                seekTo(lastOffset + 1);
            }
        } catch (Exception e) {
            logger.error("Failed to initialize consumer offset", e);
            healthy = false;
        }
    }
    
    @Override
    public List<WALEntryBatch> readNextBatch() throws Exception {
        return readNextBatch(batchSize);
    }
    
    @Override
    public List<WALEntryBatch> readNextBatch(int batchSize) throws Exception {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be at least 1, got: " + batchSize);
        }
        
        List<WALEntryBatch> batches = new ArrayList<>();
        
        try {
            for (int i = 0; i < batchSize; i++) {
                List<WALEntry> entries = new ArrayList<>();
                
                boolean hasNext = tailer.readDocument(wireIn -> {
                    // Cast WireIn to Wire if needed, or handle as WireIn
                    List<WALEntry> batch = reader.readBatch((net.openhft.chronicle.wire.Wire) wireIn);
                    entries.addAll(batch);
                });
                
                if (!hasNext || entries.isEmpty()) {
                    break; // No more data available
                }
                
                currentOffset = tailer.index();
                
                // Convert to WALEntryBatch
                WALEntryBatch batch = WALEntryBatch.withEntries(entries);
                batches.add(batch);
            }
            
            return batches;
            
        } catch (Exception e) {
            healthy = false;
            logger.error("Error reading from Chronicle Queue at offset {}", currentOffset, e);
            throw e;
        }
    }
    
    @Override
    public int getBatchSize() {
        return batchSize;
    }
    
    @Override
    public void setBatchSize(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be at least 1, got: " + batchSize);
        }
        this.batchSize = batchSize;
    }
    
    
    @Override
    public void seekTo(long offset) throws Exception {
        try {
            tailer.moveToIndex(offset);
            currentOffset = offset;
        } catch (Exception e) {
            logger.error("Failed to seek to offset {}", offset, e);
            throw e;
        }
    }
    
    @Override
    public long getCurrentOffset() {
        return currentOffset;
    }
    
    @Override
    public void commitOffset(long offset) throws Exception {
        String sql = "INSERT INTO " + offsetTable + 
                    " (consumer_id, last_offset, last_processed) VALUES (?, ?, NOW()) " +
                    "ON DUPLICATE KEY UPDATE last_offset = ?, last_processed = NOW()";
        
        try (PreparedStatement ps = offsetConnection.prepareStatement(sql)) {
            ps.setString(1, consumerName);
            ps.setLong(2, offset);
            ps.setLong(3, offset);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to commit offset {} for consumer {}", offset, consumerName, e);
            throw e;
        }
    }
    
    @Override
    public long getLastCommittedOffset() throws Exception {
        String sql = "SELECT last_offset FROM " + offsetTable + " WHERE consumer_id = ?";
        
        try (PreparedStatement ps = offsetConnection.prepareStatement(sql)) {
            ps.setString(1, consumerName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("last_offset");
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get last offset for consumer {}", consumerName, e);
            throw e;
        }
        
        return -1;
    }
    
    @Override
    public boolean isHealthy() {
        return healthy && !queue.isClosed();
    }
    
    @Override
    public boolean handleCorruption() throws Exception {
        logger.warn("Corruption detected at offset {} for consumer {}", currentOffset, consumerName);
        
        // Try progressive skip strategy
        int[] jumpSizes = {1, 10, 100, 1000, 10000};
        
        for (int jumpSize : jumpSizes) {
            try {
                long newOffset = currentOffset + jumpSize;
                logger.info("Attempting to skip {} entries, seeking to offset {}", jumpSize, newOffset);
                
                seekTo(newOffset);
                List<WALEntryBatch> batches = readNextBatch(1);
                
                if (!batches.isEmpty()) {
                    logger.info("Successfully recovered at offset {} after skipping {} entries", 
                              newOffset, jumpSize);
                    return true;
                }
            } catch (Exception e) {
                logger.debug("Failed to read at offset {} after jump of {}", 
                           currentOffset + jumpSize, jumpSize);
            }
        }
        
        logger.error("Unable to recover from corruption at offset {}", currentOffset);
        return false;
    }
    
    @Override
    public void close() throws IOException {
        try {
            if (tailer != null) {
                // Chronicle Queue tailer doesn't need explicit close
            }
            if (queue != null && !queue.isClosed()) {
                queue.close();
            }
        } catch (Exception e) {
            logger.error("Error closing ChronicleConsumer", e);
            throw new IOException("Failed to close ChronicleConsumer", e);
        }
    }
    
    private void initializeOffsetTable() throws SQLException {
        String createTable = "CREATE TABLE IF NOT EXISTS " + offsetTable + " (" +
                           "consumer_id VARCHAR(255) PRIMARY KEY, " +
                           "last_offset BIGINT NOT NULL, " +
                           "last_processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP" +
                           ")";
        
        try (PreparedStatement ps = offsetConnection.prepareStatement(createTable)) {
            ps.executeUpdate();
        }
    }
}