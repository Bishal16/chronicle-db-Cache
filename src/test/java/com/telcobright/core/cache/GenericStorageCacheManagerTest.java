package com.telcobright.core.cache;

import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.api.WALConsumer;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test GenericStorageCacheManager with unified storage for multiple entity types.
 */
public class GenericStorageCacheManagerTest {
    
    @Mock
    private WALProducer walProducer;
    
    @Mock
    private WALConsumer walConsumer;
    
    @Mock
    private DataSource dataSource;
    
    @Mock
    private Connection connection;
    
    @Mock
    private PreparedStatement preparedStatement;
    
    private GenericStorageCacheManager cacheManager;
    
    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        
        // Mock DataSource to return our mocked Connection
        when(dataSource.getConnection()).thenReturn(connection);
        
        // Mock PreparedStatement
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        
        // Mock WAL consumer to return empty list (no replay)
        when(walConsumer.readNextBatch()).thenReturn(Collections.emptyList());
        
        // Create cache manager with small capacity for testing
        Map<CacheEntityTypeSet, Integer> capacities = new HashMap<>();
        capacities.put(CacheEntityTypeSet.PACKAGE_ACCOUNT, 100);
        capacities.put(CacheEntityTypeSet.PACKAGE_ACCOUNT_RESERVE, 100);
        capacities.put(CacheEntityTypeSet.CUSTOMER, 100);
        
        cacheManager = new GenericStorageCacheManager(
            walProducer, 
            walConsumer, 
            dataSource,
            300,  // Total capacity
            capacities
        );
        
        // Initialize the cache manager
        cacheManager.initialize();
    }
    
    @Test
    void testMultiEntityBatchProcessing() throws Exception {
        // Create a batch with entries for multiple entity types
        Map<String, Object> accountData = new HashMap<>();
        accountData.put("id", 1L);
        accountData.put("name", "Gold Package");
        accountData.put("balance", 1000.0);
        
        Map<String, Object> reserveData = new HashMap<>();
        reserveData.put("id", 2L);
        reserveData.put("accountId", 1L);
        reserveData.put("amount", 200.0);
        
        Map<String, Object> customerData = new HashMap<>();
        customerData.put("id", 3L);
        customerData.put("name", "John Doe");
        customerData.put("email", "john@example.com");
        
        WALEntry accountEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("package_account")
            .operationType(WALEntry.OperationType.INSERT)
            .data(accountData)
            .build();
            
        WALEntry reserveEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("package_account_reserve")
            .operationType(WALEntry.OperationType.INSERT)
            .data(reserveData)
            .build();
            
        WALEntry customerEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("customer")
            .operationType(WALEntry.OperationType.INSERT)
            .data(customerData)
            .build();
        
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId("TXN_TEST_001")
            .addEntry(accountEntry)
            .addEntry(reserveEntry)
            .addEntry(customerEntry)
            .build();
        
        // Process the batch
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        // Verify results
        assertTrue(result.isSuccess());
        assertEquals("TXN_TEST_001", result.getTransactionId());
        assertEquals(3, result.getEntriesProcessed());
        
        // Verify WAL producer was called
        verify(walProducer, times(1)).append(batch);
        
        // Verify database transaction was managed correctly
        verify(connection, times(1)).setAutoCommit(false);
        verify(connection, times(1)).commit();
        verify(connection, times(1)).close();
        
        // Verify data is in cache (all in the same unified storage)
        Object cachedAccount = cacheManager.get("testdb", "package_account", 1L);
        assertNotNull(cachedAccount);
        
        Object cachedReserve = cacheManager.get("testdb", "package_account_reserve", 2L);
        assertNotNull(cachedReserve);
        
        Object cachedCustomer = cacheManager.get("testdb", "customer", 3L);
        assertNotNull(cachedCustomer);
    }
    
    @Test
    void testBatchRollbackOnFailure() throws Exception {
        // Setup connection to throw exception during commit
        doThrow(new SQLException("Database error")).when(connection).commit();
        
        Map<String, Object> accountData = new HashMap<>();
        accountData.put("id", 1L);
        accountData.put("name", "Test Account");
        accountData.put("balance", 500.0);
        
        WALEntry accountEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("package_account")
            .operationType(WALEntry.OperationType.INSERT)
            .data(accountData)
            .build();
            
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId("TXN_FAIL_001")
            .addEntry(accountEntry)
            .build();
        
        // Process batch - should fail
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        // Verify failure
        assertFalse(result.isSuccess());
        assertEquals("TXN_FAIL_001", result.getTransactionId());
        assertTrue(result.getErrorMessage().contains("Database error"));
        
        // Verify rollback was called
        verify(connection, times(1)).rollback();
        
        // Verify nothing in cache (not applied due to failure)
        Object cached = cacheManager.get("testdb", "package_account", 1L);
        assertNull(cached);
    }
    
    @Test
    void testEntityTypeMapping() {
        // Test table name to entity type mapping
        assertEquals(CacheEntityTypeSet.PACKAGE_ACCOUNT, 
            CacheEntityTypeSet.fromTableName("package_account"));
        assertEquals(CacheEntityTypeSet.PACKAGE_ACCOUNT_RESERVE, 
            CacheEntityTypeSet.fromTableName("package_account_reserve"));
        assertEquals(CacheEntityTypeSet.CUSTOMER, 
            CacheEntityTypeSet.fromTableName("customer"));
        assertEquals(CacheEntityTypeSet.ORDER, 
            CacheEntityTypeSet.fromTableName("orders"));
        
        // Test unknown table defaults to GENERIC_ENTITY
        assertEquals(CacheEntityTypeSet.GENERIC_ENTITY, 
            CacheEntityTypeSet.fromTableName("unknown_table"));
    }
    
    @Test
    void testUpdateOperation() throws Exception {
        // First insert an entity
        Map<String, Object> insertData = new HashMap<>();
        insertData.put("id", 5L);
        insertData.put("name", "Initial Name");
        insertData.put("value", 100);
        
        WALEntry insertEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("customer")
            .operationType(WALEntry.OperationType.INSERT)
            .data(insertData)
            .build();
            
        WALEntryBatch insertBatch = WALEntryBatch.builder()
            .transactionId("TXN_INSERT")
            .addEntry(insertEntry)
            .build();
        
        cacheManager.processBatch(insertBatch);
        
        // Now update it
        Map<String, Object> updateData = new HashMap<>();
        updateData.put("id", 5L);
        updateData.put("name", "Updated Name");
        updateData.put("value", 200);
        
        WALEntry updateEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("customer")
            .operationType(WALEntry.OperationType.UPDATE)
            .data(updateData)
            .build();
            
        WALEntryBatch updateBatch = WALEntryBatch.builder()
            .transactionId("TXN_UPDATE")
            .addEntry(updateEntry)
            .build();
        
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(updateBatch);
        
        assertTrue(result.isSuccess());
        
        // Verify update in cache
        Object cached = cacheManager.get("testdb", "customer", 5L);
        assertNotNull(cached);
    }
    
    @Test
    void testDeleteOperation() throws Exception {
        // First insert an entity
        Map<String, Object> insertData = new HashMap<>();
        insertData.put("id", 10L);
        insertData.put("name", "To Be Deleted");
        
        WALEntry insertEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("customer")
            .operationType(WALEntry.OperationType.INSERT)
            .data(insertData)
            .build();
            
        cacheManager.processBatch(WALEntryBatch.builder()
            .transactionId("TXN_INSERT")
            .addEntry(insertEntry)
            .build());
        
        // Verify it exists
        assertNotNull(cacheManager.get("testdb", "customer", 10L));
        
        // Now delete it
        Map<String, Object> deleteData = new HashMap<>();
        deleteData.put("id", 10L);
        
        WALEntry deleteEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("customer")
            .operationType(WALEntry.OperationType.DELETE)
            .data(deleteData)
            .build();
            
        WALEntryBatch deleteBatch = WALEntryBatch.builder()
            .transactionId("TXN_DELETE")
            .addEntry(deleteEntry)
            .build();
        
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(deleteBatch);
        
        assertTrue(result.isSuccess());
        
        // Verify deletion from cache
        assertNull(cacheManager.get("testdb", "customer", 10L));
    }
    
    @Test
    void testStatistics() throws Exception {
        // Process some batches
        for (int i = 0; i < 5; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("id", (long) i);
            data.put("name", "Entity " + i);
            
            WALEntry entry = new WALEntry.Builder()
                .dbName("testdb")
                .tableName("customer")
                .operationType(WALEntry.OperationType.INSERT)
                .data(data)
                .build();
                
            cacheManager.processBatch(WALEntryBatch.builder()
                .transactionId("TXN_" + i)
                .addEntry(entry)
                .build());
        }
        
        // Check statistics
        GenericStorageCacheManager.CacheStatistics stats = cacheManager.getStatistics();
        
        assertEquals(5, stats.totalBatchesProcessed);
        assertEquals(5, stats.totalEntriesProcessed);
        assertEquals(0, stats.failedBatches);
        assertEquals(5, stats.currentCacheSize);
        assertTrue(stats.replayComplete);
    }
    
    @Test
    void testAtomicityAcrossMultipleEntityTypes() throws Exception {
        // This test demonstrates the key advantage of GenericEntityStorage:
        // All entities go into the SAME storage, ensuring true atomicity
        
        Map<String, Object> account1 = new HashMap<>();
        account1.put("id", 100L);
        account1.put("balance", 1000.0);
        
        Map<String, Object> account2 = new HashMap<>();
        account2.put("id", 200L);
        account2.put("balance", 500.0);
        
        Map<String, Object> transaction = new HashMap<>();
        transaction.put("id", 300L);
        transaction.put("from", 100L);
        transaction.put("to", 200L);
        transaction.put("amount", 250.0);
        
        // Create atomic batch with different entity types
        WALEntryBatch atomicBatch = WALEntryBatch.builder()
            .transactionId("TXN_ATOMIC")
            .addEntry(new WALEntry.Builder()
                .dbName("testdb")
                .tableName("package_account")
                .operationType(WALEntry.OperationType.UPDATE)
                .data(account1)
                .build())
            .addEntry(new WALEntry.Builder()
                .dbName("testdb")
                .tableName("package_account")
                .operationType(WALEntry.OperationType.UPDATE)
                .data(account2)
                .build())
            .addEntry(new WALEntry.Builder()
                .dbName("testdb")
                .tableName("transaction_log")
                .operationType(WALEntry.OperationType.INSERT)
                .data(transaction)
                .build())
            .build();
        
        // Process the atomic batch
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(atomicBatch);
        
        // All succeed or all fail - true atomicity!
        assertTrue(result.isSuccess());
        assertEquals(3, result.getEntriesProcessed());
        
        // All entities are in the SAME unified storage
        assertNotNull(cacheManager.get("testdb", "package_account", 100L));
        assertNotNull(cacheManager.get("testdb", "package_account", 200L));
        assertNotNull(cacheManager.get("testdb", "transaction_log", 300L));
    }
}