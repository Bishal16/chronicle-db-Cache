package com.telcobright.core.cache.client.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Extended cache client interface with transaction support.
 * For operations that need to be executed atomically across multiple entities.
 */
public interface TransactionalCacheClient<K, V> extends CacheClient<K, V> {
    
    /**
     * Begin a new transaction.
     * 
     * @return Transaction ID
     */
    String beginTransaction();
    
    /**
     * Begin a new transaction with timeout.
     * 
     * @param timeoutMs Timeout in milliseconds
     * @return Transaction ID
     */
    String beginTransaction(long timeoutMs);
    
    /**
     * Commit a transaction.
     * 
     * @param transactionId Transaction ID
     * @return true if successful
     */
    boolean commit(String transactionId);
    
    /**
     * Commit a transaction asynchronously.
     * 
     * @param transactionId Transaction ID
     * @return CompletableFuture with success status
     */
    CompletableFuture<Boolean> commitAsync(String transactionId);
    
    /**
     * Rollback a transaction.
     * 
     * @param transactionId Transaction ID
     * @return true if successful
     */
    boolean rollback(String transactionId);
    
    /**
     * Execute multiple operations in a transaction.
     * 
     * @param operations List of operations
     * @return Transaction result
     */
    TransactionResult executeTransaction(List<CacheOperation<K, V>> operations);
    
    /**
     * Execute multiple operations in a transaction asynchronously.
     * 
     * @param operations List of operations
     * @return CompletableFuture with transaction result
     */
    CompletableFuture<TransactionResult> executeTransactionAsync(List<CacheOperation<K, V>> operations);
    
    /**
     * Put with transaction context.
     * 
     * @param transactionId Transaction ID
     * @param database Database name
     * @param table Table name
     * @param key Key
     * @param value Value
     * @return Previous value or null
     */
    V putTransactional(String transactionId, String database, String table, K key, V value);
    
    /**
     * Delete with transaction context.
     * 
     * @param transactionId Transaction ID
     * @param database Database name
     * @param table Table name
     * @param key Key
     * @return Deleted value or null
     */
    V deleteTransactional(String transactionId, String database, String table, K key);
    
    /**
     * Batch operations with transaction context.
     * 
     * @param transactionId Transaction ID
     * @param database Database name
     * @param operations Map of table to operations
     */
    void batchTransactional(String transactionId, String database, Map<String, List<CacheOperation<K, V>>> operations);
    
    /**
     * Get active transaction IDs.
     * 
     * @return List of active transaction IDs
     */
    List<String> getActiveTransactions();
    
    /**
     * Check if a transaction is active.
     * 
     * @param transactionId Transaction ID
     * @return true if active
     */
    boolean isTransactionActive(String transactionId);
}