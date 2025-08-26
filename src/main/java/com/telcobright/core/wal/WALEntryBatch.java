package com.telcobright.core.wal;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Batch container for WAL entries that share the same transaction ID.
 * This eliminates transaction ID duplication across individual entries.
 */
@Getter
@Setter
public class WALEntryBatch {
    private String transactionId;
    private Long timestamp;
    private List<WALEntry> entries;
    
    public WALEntryBatch() {
        this.entries = new ArrayList<>();
        this.timestamp = System.currentTimeMillis();
    }
    
    public WALEntryBatch(String transactionId) {
        this();
        this.transactionId = transactionId;
    }
    
    public WALEntryBatch(String transactionId, List<WALEntry> entries) {
        this.transactionId = transactionId;
        this.entries = new ArrayList<>(entries);
        this.timestamp = System.currentTimeMillis();
    }
    
    // Static builder method
    public static Builder builder() {
        return new Builder();
    }
    
    // Builder pattern for fluent API
    public static class Builder {
        private final WALEntryBatch batch;
        
        public Builder() {
            batch = new WALEntryBatch();
        }
        
        public Builder transactionId(String transactionId) {
            batch.transactionId = transactionId;
            return this;
        }
        
        public Builder generateTransactionId() {
            batch.transactionId = "TXN_" + System.currentTimeMillis() + "_" + UUID.randomUUID();
            return this;
        }
        
        public Builder timestamp(Long timestamp) {
            batch.timestamp = timestamp;
            return this;
        }
        
        public Builder addEntry(WALEntry entry) {
            batch.entries.add(entry);
            return this;
        }
        
        public Builder addEntries(List<WALEntry> entries) {
            batch.entries.addAll(entries);
            return this;
        }
        
        public Builder entries(List<WALEntry> entries) {
            batch.entries = new ArrayList<>(entries);
            return this;
        }
        
        public WALEntryBatch build() {
            if (batch.transactionId == null) {
                batch.transactionId = "TXN_" + System.currentTimeMillis() + "_" + UUID.randomUUID();
            }
            return batch;
        }
    }
    
    // Convenience methods
    public void addEntry(WALEntry entry) {
        this.entries.add(entry);
    }
    
    public void addEntries(List<WALEntry> entries) {
        this.entries.addAll(entries);
    }
    
    public int size() {
        return entries.size();
    }
    
    public boolean isEmpty() {
        return entries.isEmpty();
    }
    
    public WALEntry get(int index) {
        return entries.get(index);
    }
    
    /**
     * Get all entries for a specific database
     */
    public List<WALEntry> getEntriesForDatabase(String dbName) {
        return entries.stream()
            .filter(entry -> dbName.equals(entry.getDbName()))
            .toList();
    }
    
    /**
     * Get all database names in this batch
     */
    public List<String> getDatabaseNames() {
        return entries.stream()
            .map(WALEntry::getDbName)
            .distinct()
            .toList();
    }
    
    /**
     * Create a single-entry batch
     */
    public static WALEntryBatch single(String transactionId, WALEntry entry) {
        return new WALEntryBatch(transactionId, List.of(entry));
    }
    
    /**
     * Create a batch with auto-generated transaction ID
     */
    public static WALEntryBatch withEntries(List<WALEntry> entries) {
        WALEntryBatch batch = new WALEntryBatch();
        batch.transactionId = "TXN_" + System.currentTimeMillis() + "_" + UUID.randomUUID();
        batch.entries = new ArrayList<>(entries);
        return batch;
    }
    
    @Override
    public String toString() {
        return String.format("WALEntryBatch{txnId='%s', entries=%d, databases=%s}", 
            transactionId, entries.size(), getDatabaseNames());
    }
}