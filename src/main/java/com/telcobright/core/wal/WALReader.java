package com.telcobright.core.wal;

import net.openhft.chronicle.wire.Wire;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Generic WAL reader that deserializes Chronicle Queue entries to WALEntry objects.
 */
public class WALReader {
    private static final Logger logger = LoggerFactory.getLogger(WALReader.class);
    
    /**
     * Read a single WAL entry from the wire.
     * @param wire The Chronicle Queue wire to read from
     * @return The deserialized WAL entry
     */
    public WALEntry readEntry(Wire wire) {
        WALEntry entry = new WALEntry();
        
        // Check if this is a batch entry
        String transactionId = wire.read(() -> "transactionId").text();
        if (transactionId != null) {
            // This is a batch, handle differently
            return readBatchHeader(wire, transactionId);
        }
        
        // Read basic fields
        entry.setDbName(wire.read("dbName").text());
        entry.setTableName(wire.read("tableName").text());
        
        String opType = wire.read("operationType").text();
        entry.setOperationType(WALEntry.OperationType.valueOf(opType));
        
        entry.setTimestamp(wire.read("timestamp").int64());
        
        String txId = wire.read(() -> "transactionId").text();
        if (txId != null) {
            entry.setTransactionId(txId);
        }
        
        // Read data map
        int dataSize = wire.read("dataSize").int32();
        for (int i = 0; i < dataSize; i++) {
            String key = wire.read("key").text();
            String valueType = wire.read("valueType").text();
            
            Object value = readValue(wire, valueType);
            entry.put(key, value);
        }
        
        return entry;
    }
    
    /**
     * Read a batch of WAL entries.
     * @param wire The Chronicle Queue wire to read from
     * @return List of WAL entries in the batch
     */
    public List<WALEntry> readBatch(Wire wire) {
        List<WALEntry> entries = new ArrayList<>();
        
        String transactionId = wire.read("transactionId").text();
        int batchSize = wire.read("batchSize").int32();
        
        // Read each entry individually
        for (int i = 0; i < batchSize; i++) {
            final int index = i;
            wire.read("entry_" + i).marshallable(m -> {
                WALEntry entry = readEntry((Wire) m);
                entry.setTransactionId(transactionId);
                entries.add(entry);
            });
        }
        
        return entries;
    }
    
    private WALEntry readBatchHeader(Wire wire, String transactionId) {
        // Create a special batch entry that indicates a batch operation
        WALEntry batchEntry = new WALEntry();
        batchEntry.setOperationType(WALEntry.OperationType.BATCH);
        batchEntry.setTransactionId(transactionId);
        
        int batchSize = wire.read("batchSize").int32();
        batchEntry.put("batchSize", batchSize);
        
        // Store the wire reference for later batch processing
        batchEntry.put("_wire", wire);
        
        return batchEntry;
    }
    
    private Object readValue(Wire wire, String valueType) {
        switch (valueType) {
            case "NULL":
                wire.read("value").text(); // consume the value
                return null;
            case "STRING":
                return wire.read("value").text();
            case "LONG":
                return wire.read("value").int64();
            case "INT":
                return wire.read("value").int32();
            case "DOUBLE":
                return wire.read("value").float64();
            case "BOOLEAN":
                return wire.read("value").bool();
            default:
                // Default to string
                return wire.read("value").text();
        }
    }
}