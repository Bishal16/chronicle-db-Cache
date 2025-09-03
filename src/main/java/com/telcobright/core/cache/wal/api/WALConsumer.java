package com.telcobright.core.cache.wal.api;

import com.telcobright.core.wal.WALEntryBatch;
import java.io.Closeable;
import java.util.List;

/**
 * Interface for WAL consumers that read entry batches from persistent storage.
 * Consumers always work with batches, never single entries.
 */
public interface WALConsumer extends Closeable {
    
    /**
     * Reads the next batch of entries from the current position.
     * Uses the configured batch size (default 100).
     * 
     * @return List of batches up to batch size, empty list if no more available
     * @throws Exception if the read operation fails
     */
    List<WALEntryBatch> readNextBatch() throws Exception;
    
    /**
     * Reads the next batch with a specific size.
     * 
     * @param batchSize Number of entries to read (minimum 1)
     * @return List of batches up to batch size, empty list if no more available
     * @throws Exception if the read operation fails
     */
    List<WALEntryBatch> readNextBatch(int batchSize) throws Exception;
    
    /**
     * Gets the configured default batch size.
     * 
     * @return The batch size
     */
    int getBatchSize();
    
    /**
     * Sets the default batch size for future reads.
     * 
     * @param batchSize The batch size (minimum 1)
     */
    void setBatchSize(int batchSize);
    
    /**
     * Moves the consumer to a specific offset.
     * 
     * @param offset The offset to seek to
     * @throws Exception if the seek operation fails
     */
    void seekTo(long offset) throws Exception;
    
    /**
     * Gets the current read position/offset.
     * 
     * @return The current offset
     */
    long getCurrentOffset();
    
    /**
     * Commits the current offset as successfully processed.
     * 
     * @param offset The offset to commit
     * @throws Exception if the commit operation fails
     */
    void commitOffset(long offset) throws Exception;
    
    /**
     * Gets the last committed offset.
     * 
     * @return The last committed offset, or -1 if none
     * @throws Exception if unable to retrieve the offset
     */
    long getLastCommittedOffset() throws Exception;
    
    /**
     * Checks if the consumer is healthy and able to read.
     * 
     * @return true if healthy, false otherwise
     */
    boolean isHealthy();
    
    /**
     * Handles corruption at the current position.
     * Returns true if able to recover and continue, false otherwise.
     * 
     * @return true if recovered, false if unrecoverable
     * @throws Exception if recovery attempt fails
     */
    boolean handleCorruption() throws Exception;
}