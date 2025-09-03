package com.telcobright.core.cache.wal.impl;

import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.core.wal.WALWriter;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Chronicle Queue based implementation of WALProducer.
 * Wraps the existing WALWriter functionality.
 */
public class ChronicleProducer implements WALProducer {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleProducer.class);
    
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;
    private final WALWriter writer;
    private volatile boolean healthy = true;
    
    public ChronicleProducer(ChronicleQueue queue) {
        this.queue = queue;
        this.appender = queue.acquireAppender();
        this.writer = new WALWriter(appender);
    }
    
    @Override
    public long append(WALEntryBatch batch) throws Exception {
        try {
            long offset = writer.write(batch);
            if (offset < 0) {
                throw new IOException("Failed to write batch to Chronicle Queue");
            }
            return offset;
        } catch (Exception e) {
            healthy = false;
            logger.error("Error appending batch to Chronicle Queue", e);
            throw e;
        }
    }
    
    @Override
    public void flush() throws Exception {
        // Chronicle Queue auto-flushes, sync is not available in this version
        // Could force a cycle roll if needed: queue.createAppender().rollCycle()
    }
    
    @Override
    public long getCurrentOffset() {
        return appender.lastIndexAppended();
    }
    
    @Override
    public boolean isHealthy() {
        return healthy && !queue.isClosed();
    }
    
    @Override
    public void close() throws IOException {
        try {
            // Appender doesn't need explicit close in this version
            if (queue != null && !queue.isClosed()) {
                queue.close();
            }
        } catch (Exception e) {
            logger.error("Error closing ChronicleProducer", e);
            throw new IOException("Failed to close ChronicleProducer", e);
        }
    }
}