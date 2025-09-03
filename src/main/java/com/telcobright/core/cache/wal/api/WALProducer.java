package com.telcobright.core.cache.wal.api;

import com.telcobright.core.wal.WALEntryBatch;
import java.io.Closeable;

/**
 * Interface for WAL producers that write entry batches to persistent storage.
 */
public interface WALProducer extends Closeable {
    
    /**
     * Appends a batch of WAL entries to the log.
     * 
     * @param batch The batch of entries to append
     * @return The offset/index where the batch was written
     * @throws Exception if the write operation fails
     */
    long append(WALEntryBatch batch) throws Exception;
    
    /**
     * Flushes any buffered entries to persistent storage.
     * 
     * @throws Exception if the flush operation fails
     */
    void flush() throws Exception;
    
    /**
     * Gets the current write position/offset.
     * 
     * @return The current offset
     */
    long getCurrentOffset();
    
    /**
     * Checks if the producer is healthy and able to write.
     * 
     * @return true if healthy, false otherwise
     */
    boolean isHealthy();
}