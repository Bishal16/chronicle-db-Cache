package com.telcobright.core.cache.wal.processor;

import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.core.wal.WALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Processes WAL entry batches atomically with transactional guarantees.
 * Ensures all entries in a batch are either fully committed or fully rolled back.
 */
public class BatchProcessor {
    private static final Logger logger = LoggerFactory.getLogger(BatchProcessor.class);
    
    private final WALConsumer consumer;
    private final Connection connection;
    private final BiConsumer<WALEntry, Connection> entryProcessor;
    private final int defaultBatchSize;
    
    public BatchProcessor(WALConsumer consumer, Connection connection, 
                         BiConsumer<WALEntry, Connection> entryProcessor) {
        this(consumer, connection, entryProcessor, 100);
    }
    
    public BatchProcessor(WALConsumer consumer, Connection connection,
                         BiConsumer<WALEntry, Connection> entryProcessor, int defaultBatchSize) {
        this.consumer = consumer;
        this.connection = connection;
        this.entryProcessor = entryProcessor;
        this.defaultBatchSize = defaultBatchSize;
        
        // Configure consumer batch size
        consumer.setBatchSize(defaultBatchSize);
    }
    
    /**
     * Processes the next batch of entries atomically.
     * All entries in the batch are processed in a single transaction.
     * 
     * @return Number of entries processed, 0 if no entries available
     * @throws Exception if processing fails
     */
    public int processNextBatch() throws Exception {
        return processNextBatch(defaultBatchSize);
    }
    
    /**
     * Processes the next batch with a specific size.
     * 
     * @param batchSize Number of batches to read
     * @return Number of entries processed
     * @throws Exception if processing fails
     */
    public int processNextBatch(int batchSize) throws Exception {
        List<WALEntryBatch> batches = consumer.readNextBatch(batchSize);
        
        if (batches.isEmpty()) {
            logger.debug("No batches available to process");
            return 0;
        }
        
        int totalProcessed = 0;
        
        try {
            // Start transaction
            connection.setAutoCommit(false);
            
            // Process all batches in single transaction
            for (WALEntryBatch batch : batches) {
                logger.debug("Processing batch with {} entries", batch.getEntries().size());
                
                // Process each entry in the batch
                for (WALEntry entry : batch.getEntries()) {
                    try {
                        entryProcessor.accept(entry, connection);
                        totalProcessed++;
                    } catch (Exception e) {
                        logger.error("Failed to process entry: {}", entry, e);
                        throw e; // Will trigger rollback
                    }
                }
            }
            
            // Commit offset within same transaction
            // Use consumer's current offset after reading batches
            long currentOffset = consumer.getCurrentOffset();
            if (currentOffset >= 0) {
                consumer.commitOffset(currentOffset);
            }
            
            // Commit transaction
            connection.commit();
            logger.info("Successfully processed {} entries in atomic transaction", totalProcessed);
            
            return totalProcessed;
            
        } catch (Exception e) {
            // Rollback on any failure
            try {
                connection.rollback();
                logger.error("Rolled back transaction due to processing error", e);
            } catch (SQLException rollbackEx) {
                logger.error("Failed to rollback transaction", rollbackEx);
            }
            throw e;
        } finally {
            // Restore auto-commit
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                logger.error("Failed to restore auto-commit", e);
            }
        }
    }
    
    /**
     * Continuously processes batches until stopped or no more data.
     * 
     * @param stopCondition Function that returns true when processing should stop
     * @throws Exception if processing fails
     */
    public void processContinuously(java.util.function.BooleanSupplier stopCondition) throws Exception {
        while (!stopCondition.getAsBoolean()) {
            int processed = processNextBatch();
            
            if (processed == 0) {
                // No data available, wait briefly
                Thread.sleep(100);
            }
        }
    }
    
    /**
     * Processes all available batches up to a specific offset.
     * 
     * @param targetOffset The offset to process up to
     * @return Total number of entries processed
     * @throws Exception if processing fails
     */
    public int processUntilOffset(long targetOffset) throws Exception {
        int totalProcessed = 0;
        
        while (consumer.getCurrentOffset() < targetOffset) {
            int processed = processNextBatch();
            
            if (processed == 0) {
                break; // No more data
            }
            
            totalProcessed += processed;
        }
        
        return totalProcessed;
    }
}