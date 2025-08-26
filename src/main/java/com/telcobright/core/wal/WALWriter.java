package com.telcobright.core.wal;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.wire.WireOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * WAL writer that writes WALEntryBatch for transactional consistency.
 * Transaction ID is managed at the batch level, eliminating duplication.
 */
public class WALWriter {
    private static final Logger logger = LoggerFactory.getLogger(WALWriter.class);
    private final ExcerptAppender appender;
    
    public WALWriter(ExcerptAppender appender) {
        this.appender = appender;
    }
    
    /**
     * Write WALEntryBatch as a transaction.
     * This is the PRIMARY write method - uses WALEntryBatch for transaction management.
     * 
     * @param batch The WALEntryBatch to write
     * @return The index where the batch was written
     */
    public long write(WALEntryBatch batch) {
        if (batch == null || batch.isEmpty()) {
            logger.warn("Attempted to write empty WALEntryBatch to WAL");
            return -1;
        }
        
        String transactionId = batch.getTransactionId();
        if (transactionId == null) {
            transactionId = "TXN_" + System.currentTimeMillis() + "_" + UUID.randomUUID();
            batch.setTransactionId(transactionId);
        }
        
        final String txId = transactionId;
        final List<WALEntry> entries = batch.getEntries();
        
        appender.writeDocument(w -> {
            w.write("transactionId").text(txId);
            w.write("batchSize").int32(entries.size());
            w.write("timestamp").int64(batch.getTimestamp() != null ? batch.getTimestamp() : System.currentTimeMillis());
            
            // Write each entry in the batch
            for (int i = 0; i < entries.size(); i++) {
                final int idx = i;
                w.write("entry_" + i).marshallable(m -> writeEntry(m, entries.get(idx)));
            }
        });
        
        long index = appender.lastIndexAppended();
        
        if (entries.size() == 1) {
            logger.debug("Wrote single entry batch at index: {} for {}.{}, txId: {}", 
                index, entries.get(0).getDbName(), entries.get(0).getTableName(), txId);
        } else {
            logger.debug("Wrote batch of {} entries at index: {}, txId: {}, databases: {}", 
                entries.size(), index, txId, batch.getDatabaseNames());
        }
        
        return index;
    }
    
    /**
     * Write WAL entries as a batch transaction (Legacy support).
     * Creates a WALEntryBatch internally for backward compatibility.
     * 
     * @param entries The WAL entries to write (can be a single entry)
     * @return The index where the batch was written
     */
    public long write(List<WALEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            logger.warn("Attempted to write empty batch to WAL");
            return -1;
        }
        
        // Create WALEntryBatch from List<WALEntry>
        WALEntryBatch batch = WALEntryBatch.withEntries(entries);
        return write(batch);
    }
    
    /**
     * Convenience method to write a single entry as a batch of size 1
     */
    public long write(WALEntry entry) {
        WALEntryBatch batch = WALEntryBatch.builder()
            .generateTransactionId()
            .addEntry(entry)
            .build();
        return write(batch);
    }
    
    /**
     * Write a single entry with a specific transaction ID
     */
    public long write(String transactionId, WALEntry entry) {
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId(transactionId)
            .addEntry(entry)
            .build();
        return write(batch);
    }
    
    /**
     * Write the contents of a single WAL entry
     */
    private void writeEntry(WireOut wire, WALEntry entry) {
        wire.write("dbName").text(entry.getDbName());
        wire.write("tableName").text(entry.getTableName());
        wire.write("operationType").text(entry.getOperationType().name());
        
        // Write data map
        Map<String, Object> data = entry.getData();
        wire.write("dataSize").int32(data.size());
        
        for (Map.Entry<String, Object> dataEntry : data.entrySet()) {
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