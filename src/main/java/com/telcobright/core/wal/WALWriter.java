package com.telcobright.core.wal;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Generic WAL writer that serializes WALEntry objects to Chronicle Queue.
 */
public class WALWriter {
    private static final Logger logger = LoggerFactory.getLogger(WALWriter.class);
    private final ExcerptAppender appender;
    
    public WALWriter(ExcerptAppender appender) {
        this.appender = appender;
    }
    
    /**
     * Write a WAL entry to the Chronicle Queue.
     * @param entry The WAL entry to writePackageAccountCache cacheManager;  // Injected by Quarkus
     * @return The index where the entry was written
     */
    public long write(WALEntry entry) {
        appender.writeDocument(w -> {
            writeEntry(w, entry);
        });
        
        long index = appender.lastIndexAppended();
        logger.debug("Wrote WAL entry at index: {} for {}.{} operation: {}", 
            index, entry.getDbName(), entry.getTableName(), entry.getOperationType());
        return index;
    }
    
    /**
     * Write multiple WAL entries as a batch transaction.
     * @param entries The WAL entries to write
     * @param transactionId Transaction ID for the batch
     * @return The index where the batch was written
     */
    public long writeBatch(WALEntry[] entries, String transactionId) {
        appender.writeDocument(w -> {
            w.write("transactionId").text(transactionId);
            w.write("batchSize").int32(entries.length);
            
            // Write each entry individually instead of using array
            for (int i = 0; i < entries.length; i++) {
                final int idx = i;
                w.write("entry_" + i).marshallable(m -> writeEntry(m, entries[idx]));
            }
        });
        
        long index = appender.lastIndexAppended();
        logger.debug("Wrote batch WAL entry at index: {} with {} entries, txId: {}", 
            index, entries.length, transactionId);
        return index;
    }
    
    private void writeEntry(WireOut wire, WALEntry entry) {
        wire.write("dbName").text(entry.getDbName());
        wire.write("tableName").text(entry.getTableName());
        wire.write("operationType").text(entry.getOperationType().name());
        wire.write("timestamp").int64(entry.getTimestamp());
        
        if (entry.getTransactionId() != null) {
            wire.write("transactionId").text(entry.getTransactionId());
        }
        
        // Write data map
        wire.write("dataSize").int32(entry.getData().size());
        for (Map.Entry<String, Object> dataEntry : entry.getData().entrySet()) {
            wire.write("key").text(dataEntry.getKey());
            
            Object value = dataEntry.getValue();
            if (value == null) {
                wire.write("valueType").text("NULL");
                wire.write("value").text("");
            } else if (value instanceof String) {
                wire.write("valueType").text("STRING");
                wire.write("value").text((String) value);
            } else if (value instanceof Long) {
                wire.write("valueType").text("LONG");
                wire.write("value").int64((Long) value);
            } else if (value instanceof Integer) {
                wire.write("valueType").text("INT");
                wire.write("value").int32((Integer) value);
            } else if (value instanceof Double) {
                wire.write("valueType").text("DOUBLE");
                wire.write("value").float64((Double) value);
            } else if (value instanceof Boolean) {
                wire.write("valueType").text("BOOLEAN");
                wire.write("value").bool((Boolean) value);
            } else {
                // For complex types, convert to string
                wire.write("valueType").text("STRING");
                wire.write("value").text(value.toString());
            }
        }
    }
}