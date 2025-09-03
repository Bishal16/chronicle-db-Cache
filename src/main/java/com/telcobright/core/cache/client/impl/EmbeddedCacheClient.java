package com.telcobright.core.cache.client.impl;

import com.telcobright.core.cache.client.api.*;
import com.telcobright.core.cache.EntityCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Embedded cache client implementation for direct in-process access.
 * Directly accesses the EntityCache instances without network overhead.
 */
@SuppressWarnings("unchecked")
public class EmbeddedCacheClient<K, V> implements TransactionalCacheClient<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedCacheClient.class);
    
    // Registry of all entity caches by table name
    private final Map<String, EntityCache<K, V>> cacheRegistry;
    
    // Transaction management
    private final Map<String, Transaction> activeTransactions = new ConcurrentHashMap<>();
    
    public EmbeddedCacheClient(Map<String, EntityCache<K, V>> cacheRegistry) {
        this.cacheRegistry = cacheRegistry;
        logger.info("Initialized EmbeddedCacheClient with {} caches", cacheRegistry.size());
    }
    
    @Override
    public V get(String database, String table, K key) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            logger.warn("Cache not found for table: {}", table);
            return null;
        }
        return cache.get(database, key);
    }
    
    @Override
    public CompletableFuture<V> getAsync(String database, String table, K key) {
        return CompletableFuture.supplyAsync(() -> get(database, table, key));
    }
    
    @Override
    public Map<K, V> getBatch(String database, String table, List<K> keys) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            return Collections.emptyMap();
        }
        
        Map<K, V> result = new HashMap<>();
        for (K key : keys) {
            V value = cache.get(database, key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }
    
    @Override
    public Map<K, V> getAll(String database, String table) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            return Collections.emptyMap();
        }
        return cache.getAll(database);
    }
    
    @Override
    public V put(String database, String table, K key, V value) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            logger.warn("Cache not found for table: {}", table);
            return null;
        }
        
        V previousValue = cache.get(database, key);
        cache.put(database, value);
        return previousValue;
    }
    
    @Override
    public CompletableFuture<V> putAsync(String database, String table, K key, V value) {
        return CompletableFuture.supplyAsync(() -> put(database, table, key, value));
    }
    
    @Override
    public void putBatch(String database, String table, Map<K, V> entries) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            logger.warn("Cache not found for table: {}", table);
            return;
        }
        
        List<V> values = new ArrayList<>(entries.values());
        Map<String, List<V>> batchMap = new HashMap<>();
        batchMap.put(database, values);
        
        String transactionId = "TXN_" + System.currentTimeMillis();
        cache.applyInserts(batchMap, transactionId);
    }
    
    @Override
    public boolean update(String database, String table, K key, V value) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            return false;
        }
        
        if (!cache.contains(database, key)) {
            return false;
        }
        
        cache.update(database, key, value);
        return true;
    }
    
    @Override
    public V delete(String database, String table, K key) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            return null;
        }
        
        V value = cache.get(database, key);
        if (value != null) {
            cache.delete(database, key);
        }
        return value;
    }
    
    @Override
    public Map<K, V> deleteBatch(String database, String table, List<K> keys) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            return Collections.emptyMap();
        }
        
        Map<K, V> deleted = new HashMap<>();
        for (K key : keys) {
            V value = cache.get(database, key);
            if (value != null) {
                deleted.put(key, value);
            }
        }
        
        Map<String, List<K>> deleteMap = new HashMap<>();
        deleteMap.put(database, keys);
        
        String transactionId = "TXN_" + System.currentTimeMillis();
        cache.applyDeletes(deleteMap, transactionId);
        
        return deleted;
    }
    
    @Override
    public boolean contains(String database, String table, K key) {
        EntityCache<K, V> cache = getCache(table);
        return cache != null && cache.contains(database, key);
    }
    
    @Override
    public int size(String database, String table) {
        EntityCache<K, V> cache = getCache(table);
        return cache != null ? cache.size(database) : 0;
    }
    
    @Override
    public void clear(String database, String table) {
        EntityCache<K, V> cache = getCache(table);
        if (cache != null) {
            cache.clear(database);
        }
    }
    
    @Override
    public List<V> query(String database, String table, QueryFilter<V> filter) {
        EntityCache<K, V> cache = getCache(table);
        if (cache == null) {
            return Collections.emptyList();
        }
        
        return cache.getAll(database).values().stream()
            .filter(filter::test)
            .collect(Collectors.toList());
    }
    
    @Override
    public void close() {
        activeTransactions.clear();
        logger.info("EmbeddedCacheClient closed");
    }
    
    @Override
    public boolean isHealthy() {
        return true; // Always healthy for embedded
    }
    
    @Override
    public ClientType getType() {
        return ClientType.EMBEDDED;
    }
    
    // Transaction support
    
    @Override
    public String beginTransaction() {
        return beginTransaction(60000); // Default 60 seconds timeout
    }
    
    @Override
    public String beginTransaction(long timeoutMs) {
        String transactionId = "TXN_" + UUID.randomUUID();
        Transaction txn = new Transaction(transactionId, timeoutMs);
        activeTransactions.put(transactionId, txn);
        logger.debug("Started transaction: {}", transactionId);
        return transactionId;
    }
    
    @Override
    public boolean commit(String transactionId) {
        Transaction txn = activeTransactions.remove(transactionId);
        if (txn == null) {
            logger.warn("Transaction not found: {}", transactionId);
            return false;
        }
        
        try {
            // Execute all operations
            for (CacheOperation<K, V> op : txn.operations) {
                executeOperation(op);
            }
            logger.debug("Committed transaction: {}", transactionId);
            return true;
        } catch (Exception e) {
            logger.error("Failed to commit transaction: {}", transactionId, e);
            return false;
        }
    }
    
    @Override
    public CompletableFuture<Boolean> commitAsync(String transactionId) {
        return CompletableFuture.supplyAsync(() -> commit(transactionId));
    }
    
    @Override
    public boolean rollback(String transactionId) {
        Transaction txn = activeTransactions.remove(transactionId);
        if (txn == null) {
            logger.warn("Transaction not found: {}", transactionId);
            return false;
        }
        
        logger.debug("Rolled back transaction: {}", transactionId);
        return true;
    }
    
    @Override
    public TransactionResult executeTransaction(List<CacheOperation<K, V>> operations) {
        String transactionId = beginTransaction();
        long startTime = System.currentTimeMillis();
        
        TransactionResult.Builder resultBuilder = TransactionResult.builder()
            .transactionId(transactionId);
        
        try {
            for (int i = 0; i < operations.size(); i++) {
                CacheOperation<K, V> op = operations.get(i);
                try {
                    Object result = executeOperation(op);
                    resultBuilder.addOperationResult(
                        new TransactionResult.OperationResult(i, true, result, null)
                    );
                } catch (Exception e) {
                    resultBuilder.addOperationResult(
                        new TransactionResult.OperationResult(i, false, null, e.getMessage())
                    );
                    throw e;
                }
            }
            
            resultBuilder.success(true);
        } catch (Exception e) {
            rollback(transactionId);
            resultBuilder.success(false)
                        .errorMessage(e.getMessage());
        }
        
        resultBuilder.executionTimeMs(System.currentTimeMillis() - startTime);
        return resultBuilder.build();
    }
    
    @Override
    public CompletableFuture<TransactionResult> executeTransactionAsync(List<CacheOperation<K, V>> operations) {
        return CompletableFuture.supplyAsync(() -> executeTransaction(operations));
    }
    
    @Override
    public V putTransactional(String transactionId, String database, String table, K key, V value) {
        Transaction txn = activeTransactions.get(transactionId);
        if (txn == null) {
            throw new IllegalArgumentException("Transaction not found: " + transactionId);
        }
        
        CacheOperation<K, V> op = CacheOperation.put(database, table, key, value);
        txn.operations.add(op);
        
        return get(database, table, key);
    }
    
    @Override
    public V deleteTransactional(String transactionId, String database, String table, K key) {
        Transaction txn = activeTransactions.get(transactionId);
        if (txn == null) {
            throw new IllegalArgumentException("Transaction not found: " + transactionId);
        }
        
        CacheOperation<K, V> op = CacheOperation.delete(database, table, key);
        txn.operations.add(op);
        
        return get(database, table, key);
    }
    
    @Override
    public void batchTransactional(String transactionId, String database, 
                                  Map<String, List<CacheOperation<K, V>>> operations) {
        Transaction txn = activeTransactions.get(transactionId);
        if (txn == null) {
            throw new IllegalArgumentException("Transaction not found: " + transactionId);
        }
        
        for (List<CacheOperation<K, V>> ops : operations.values()) {
            txn.operations.addAll(ops);
        }
    }
    
    @Override
    public List<String> getActiveTransactions() {
        return new ArrayList<>(activeTransactions.keySet());
    }
    
    @Override
    public boolean isTransactionActive(String transactionId) {
        return activeTransactions.containsKey(transactionId);
    }
    
    // Helper methods
    
    private EntityCache<K, V> getCache(String table) {
        return cacheRegistry.get(table);
    }
    
    private Object executeOperation(CacheOperation<K, V> op) {
        switch (op.getType()) {
            case GET:
                return get(op.getDatabase(), op.getTable(), op.getKey());
            case PUT:
                return put(op.getDatabase(), op.getTable(), op.getKey(), op.getValue());
            case UPDATE:
                return update(op.getDatabase(), op.getTable(), op.getKey(), op.getValue());
            case DELETE:
                return delete(op.getDatabase(), op.getTable(), op.getKey());
            case CONTAINS:
                return contains(op.getDatabase(), op.getTable(), op.getKey());
            default:
                throw new UnsupportedOperationException("Operation type not supported: " + op.getType());
        }
    }
    
    // Inner class for transaction state
    private static class Transaction {
        final String id;
        final long timeoutMs;
        final long startTime;
        final List<CacheOperation> operations;
        
        Transaction(String id, long timeoutMs) {
            this.id = id;
            this.timeoutMs = timeoutMs;
            this.startTime = System.currentTimeMillis();
            this.operations = new ArrayList<>();
        }
        
        boolean isExpired() {
            return System.currentTimeMillis() - startTime > timeoutMs;
        }
    }
}