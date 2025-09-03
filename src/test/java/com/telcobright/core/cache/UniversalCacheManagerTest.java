package com.telcobright.core.cache;

import com.telcobright.core.cache.universal.*;
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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Test UniversalCacheManager with multi-entity batches.
 */
public class UniversalCacheManagerTest {
    
    @Mock
    private WALProducer walProducer;
    
    @Mock
    private WALConsumer walConsumer;
    
    @Mock
    private DataSource dataSource;
    
    @Mock
    private Connection connection;
    
    private UniversalCacheManager cacheManager;
    
    @BeforeEach
    void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        
        // Mock DataSource to return our mocked Connection
        when(dataSource.getConnection()).thenReturn(connection);
        
        // Create cache manager
        cacheManager = new UniversalCacheManager(walProducer, walConsumer, dataSource);
        
        // Initialize the cache manager
        cacheManager.initialize();
    }
    
    @Test
    void testMultiEntityBatchProcessing() throws Exception {
        // Register handlers for two different entity types
        TestEntityHandler accountHandler = new TestEntityHandler("package_account");
        TestEntityHandler reserveHandler = new TestEntityHandler("package_account_reserve");
        
        cacheManager.registerHandler(accountHandler);
        cacheManager.registerHandler(reserveHandler);
        
        // Create a batch with entries for both entities
        Map<String, Object> accountData = new HashMap<>();
        accountData.put("id", 1L);
        accountData.put("name", "Test Account");
        accountData.put("balance", 100.0);
        
        Map<String, Object> reserveData = new HashMap<>();
        reserveData.put("id", 2L);
        reserveData.put("accountId", 1L);
        reserveData.put("amount", 50.0);
        
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
        
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId("TXN_TEST_001")
            .addEntry(accountEntry)
            .addEntry(reserveEntry)
            .build();
        
        // Process the batch
        UniversalCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        // Verify results
        assertTrue(result.isSuccess());
        assertEquals("TXN_TEST_001", result.getTransactionId());
        assertEquals(2, result.getEntriesProcessed());
        
        // Verify WAL producer was called with the complete batch
        verify(walProducer, times(1)).append(batch);
        
        // Verify database transaction was committed
        verify(connection, times(1)).setAutoCommit(false);
        verify(connection, times(1)).commit();
        verify(connection, times(1)).close();
        
        // Verify both handlers were called
        assertEquals(1, accountHandler.processCalls);
        assertEquals(1, reserveHandler.processCalls);
        
        // Verify data is in cache
        Object cachedAccount = cacheManager.get("testdb", "package_account", 1L);
        assertNotNull(cachedAccount);
        
        Object cachedReserve = cacheManager.get("testdb", "package_account_reserve", 2L);
        assertNotNull(cachedReserve);
    }
    
    @Test
    void testBatchRollbackOnFailure() throws Exception {
        // Register handler that will fail
        FailingEntityHandler failHandler = new FailingEntityHandler("failing_table");
        cacheManager.registerHandler(failHandler);
        
        // Create entry for failing handler
        WALEntry failEntry = new WALEntry.Builder()
            .dbName("testdb")
            .tableName("failing_table")
            .operationType(WALEntry.OperationType.INSERT)
            .data(Map.of("id", 1L))
            .build();
            
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId("TXN_FAIL_001")
            .addEntry(failEntry)
            .build();
        
        // Process batch - should fail
        UniversalCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        // Verify failure
        assertFalse(result.isSuccess());
        assertEquals("TXN_FAIL_001", result.getTransactionId());
        assertTrue(result.getErrorMessage().contains("Simulated database failure"));
        
        // Verify rollback was called
        verify(connection, times(1)).rollback();
        
        // Verify nothing in cache (rolled back)
        Object cached = cacheManager.get("testdb", "failing_table", 1L);
        assertNull(cached);
    }
    
    /**
     * Test entity handler that tracks calls.
     */
    static class TestEntityHandler extends GenericEntityHandler<Long, Map<String, Object>> {
        int processCalls = 0;
        
        @SuppressWarnings("unchecked")
        public TestEntityHandler(String tableName) {
            super(tableName, (Class<Map<String, Object>>) (Class<?>) Map.class, "id");
        }
        
        @Override
        protected Long extractKey(WALEntry entry) {
            Object id = entry.getData().get("id");
            return id instanceof Long ? (Long) id : Long.valueOf(id.toString());
        }
        
        @Override
        protected Map<String, Object> convertToEntity(WALEntry entry) {
            return entry.getData();
        }
        
        @Override
        public void applyToDatabase(String dbName, WALEntry entry, Connection connection) throws Exception {
            processCalls++;
            // Simulate successful database update
        }
    }
    
    /**
     * Entity handler that always fails database updates.
     */
    static class FailingEntityHandler extends TestEntityHandler {
        public FailingEntityHandler(String tableName) {
            super(tableName);
        }
        
        @Override
        public void applyToDatabase(String dbName, WALEntry entry, Connection connection) throws Exception {
            throw new SQLException("Simulated database failure");
        }
    }
}