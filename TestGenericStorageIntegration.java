import com.telcobright.core.cache.*;
import com.telcobright.core.cache.wal.api.*;
import com.telcobright.core.wal.*;
import java.util.*;

public class TestGenericStorageIntegration {
    
    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("Testing GenericStorageCacheManager");
        System.out.println("========================================");
        
        // Create mock WAL components
        WALProducer walProducer = new WALProducer() {
            public long append(WALEntryBatch batch) {
                System.out.println("WAL: Written batch " + batch.getTransactionId() + " with " + batch.size() + " entries");
                return 0;
            }
            public void flush() {}
            public long getCurrentOffset() { return 0; }
            public boolean isHealthy() { return true; }
            public void close() {}
        };
        
        WALConsumer walConsumer = new WALConsumer() {
            public List<WALEntryBatch> readNextBatch() throws Exception {
                return Collections.emptyList();
            }
            public List<WALEntryBatch> readNextBatch(int batchSize) throws Exception {
                return Collections.emptyList();
            }
            public int getBatchSize() { return 100; }
            public void setBatchSize(int batchSize) {}
            public void seekTo(long offset) throws Exception {}
            public long getCurrentOffset() { return 0; }
            public void commitOffset(long offset) throws Exception {}
            public long getLastCommittedOffset() throws Exception { return -1; }
            public boolean handleCorruption() throws Exception {
                return false;
            }
            public boolean isHealthy() {
                return true;
            }
            public void close() {}
        };
        
        // Create mock DataSource
        javax.sql.DataSource dataSource = new javax.sql.DataSource() {
            public java.sql.Connection getConnection() throws java.sql.SQLException {
                return new MockConnection();
            }
            public java.sql.Connection getConnection(String username, String password) {
                return new MockConnection();
            }
            public java.io.PrintWriter getLogWriter() { return null; }
            public void setLogWriter(java.io.PrintWriter out) {}
            public void setLoginTimeout(int seconds) {}
            public int getLoginTimeout() { return 0; }
            public java.util.logging.Logger getParentLogger() { return null; }
            public <T> T unwrap(Class<T> iface) { return null; }
            public boolean isWrapperFor(Class<?> iface) { return false; }
        };
        
        // Create cache manager with small capacity
        Map<CacheEntityTypeSet, Integer> capacities = new HashMap<>();
        capacities.put(CacheEntityTypeSet.PACKAGE_ACCOUNT, 100);
        capacities.put(CacheEntityTypeSet.PACKAGE_ACCOUNT_RESERVE, 100);
        capacities.put(CacheEntityTypeSet.CUSTOMER, 100);
        
        GenericStorageCacheManager cacheManager = new GenericStorageCacheManager(
            walProducer, 
            walConsumer, 
            dataSource,
            300,  // Total capacity
            capacities
        );
        
        // Initialize
        System.out.println("\n1. Initializing cache manager...");
        cacheManager.initialize();
        System.out.println("   ✓ Cache initialized");
        
        // Test 1: Multi-entity batch
        System.out.println("\n2. Testing multi-entity batch processing...");
        
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
        
        GenericStorageCacheManager.BatchProcessResult result = cacheManager.processBatch(batch);
        
        System.out.println("   Transaction ID: " + result.getTransactionId());
        System.out.println("   Success: " + result.isSuccess());
        System.out.println("   Entries processed: " + result.getEntriesProcessed());
        
        // Test 2: Verify data in cache
        System.out.println("\n3. Verifying data in unified storage...");
        
        Object cachedAccount = cacheManager.get("testdb", "package_account", 1L);
        System.out.println("   PackageAccount (id=1): " + (cachedAccount != null ? "✓ Found" : "✗ Not found"));
        
        Object cachedReserve = cacheManager.get("testdb", "package_account_reserve", 2L);
        System.out.println("   PackageAccountReserve (id=2): " + (cachedReserve != null ? "✓ Found" : "✗ Not found"));
        
        Object cachedCustomer = cacheManager.get("testdb", "customer", 3L);
        System.out.println("   Customer (id=3): " + (cachedCustomer != null ? "✓ Found" : "✗ Not found"));
        
        // Test 3: Statistics
        System.out.println("\n4. Cache Statistics:");
        GenericStorageCacheManager.CacheStatistics stats = cacheManager.getStatistics();
        System.out.println("   Total batches processed: " + stats.totalBatchesProcessed);
        System.out.println("   Total entries processed: " + stats.totalEntriesProcessed);
        System.out.println("   Current cache size: " + stats.currentCacheSize);
        System.out.println("   Failed batches: " + stats.failedBatches);
        
        System.out.println("\n========================================");
        System.out.println("✓ All tests completed successfully!");
        System.out.println("========================================");
    }
    
    // Mock Connection class
    static class MockConnection implements java.sql.Connection {
        public void setAutoCommit(boolean autoCommit) {}
        public void commit() {}
        public void rollback() {}
        public void close() {}
        public boolean isClosed() { return false; }
        public java.sql.PreparedStatement prepareStatement(String sql) { 
            return new MockPreparedStatement(); 
        }
        // ... other methods return null/default
        public java.sql.Statement createStatement() { return null; }
        public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) { return null; }
        public java.sql.CallableStatement prepareCall(String sql) { return null; }
        public String nativeSQL(String sql) { return sql; }
        public boolean getAutoCommit() { return false; }
        public java.sql.DatabaseMetaData getMetaData() { return null; }
        public void setReadOnly(boolean readOnly) {}
        public boolean isReadOnly() { return false; }
        public void setCatalog(String catalog) {}
        public String getCatalog() { return null; }
        public void setTransactionIsolation(int level) {}
        public int getTransactionIsolation() { return 0; }
        public java.sql.SQLWarning getWarnings() { return null; }
        public void clearWarnings() {}
        public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) { return null; }
        public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) { return null; }
        public java.util.Map<String,Class<?>> getTypeMap() { return null; }
        public void setTypeMap(java.util.Map<String,Class<?>> map) {}
        public void setHoldability(int holdability) {}
        public int getHoldability() { return 0; }
        public java.sql.Savepoint setSavepoint() { return null; }
        public java.sql.Savepoint setSavepoint(String name) { return null; }
        public void rollback(java.sql.Savepoint savepoint) {}
        public void releaseSavepoint(java.sql.Savepoint savepoint) {}
        public java.sql.Statement createStatement(int a, int b, int c) { return null; }
        public java.sql.PreparedStatement prepareStatement(String sql, int a, int b, int c) { return null; }
        public java.sql.CallableStatement prepareCall(String sql, int a, int b, int c) { return null; }
        public java.sql.PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) { return null; }
        public java.sql.PreparedStatement prepareStatement(String sql, int[] columnIndexes) { return null; }
        public java.sql.PreparedStatement prepareStatement(String sql, String[] columnNames) { return null; }
        public java.sql.Clob createClob() { return null; }
        public java.sql.Blob createBlob() { return null; }
        public java.sql.NClob createNClob() { return null; }
        public java.sql.SQLXML createSQLXML() { return null; }
        public boolean isValid(int timeout) { return true; }
        public void setClientInfo(String name, String value) {}
        public void setClientInfo(java.util.Properties properties) {}
        public String getClientInfo(String name) { return null; }
        public java.util.Properties getClientInfo() { return null; }
        public java.sql.Array createArrayOf(String typeName, Object[] elements) { return null; }
        public java.sql.Struct createStruct(String typeName, Object[] attributes) { return null; }
        public void setSchema(String schema) {}
        public String getSchema() { return null; }
        public void abort(java.util.concurrent.Executor executor) {}
        public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) {}
        public int getNetworkTimeout() { return 0; }
        public <T> T unwrap(Class<T> iface) { return null; }
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }
    
    // Mock PreparedStatement
    static class MockPreparedStatement implements java.sql.PreparedStatement {
        public int executeUpdate() { return 1; }
        public void setObject(int parameterIndex, Object x) {}
        public void close() {}
        // ... other methods return null/default
        public java.sql.ResultSet executeQuery() { return null; }
        public void setNull(int parameterIndex, int sqlType) {}
        public void setBoolean(int parameterIndex, boolean x) {}
        public void setByte(int parameterIndex, byte x) {}
        public void setShort(int parameterIndex, short x) {}
        public void setInt(int parameterIndex, int x) {}
        public void setLong(int parameterIndex, long x) {}
        public void setFloat(int parameterIndex, float x) {}
        public void setDouble(int parameterIndex, double x) {}
        public void setBigDecimal(int parameterIndex, java.math.BigDecimal x) {}
        public void setString(int parameterIndex, String x) {}
        public void setBytes(int parameterIndex, byte[] x) {}
        public void setDate(int parameterIndex, java.sql.Date x) {}
        public void setTime(int parameterIndex, java.sql.Time x) {}
        public void setTimestamp(int parameterIndex, java.sql.Timestamp x) {}
        public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) {}
        public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) {}
        public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) {}
        public void clearParameters() {}
        public void setObject(int parameterIndex, Object x, int targetSqlType) {}
        public boolean execute() { return true; }
        public void addBatch() {}
        public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) {}
        public void setRef(int parameterIndex, java.sql.Ref x) {}
        public void setBlob(int parameterIndex, java.sql.Blob x) {}
        public void setClob(int parameterIndex, java.sql.Clob x) {}
        public void setArray(int parameterIndex, java.sql.Array x) {}
        public java.sql.ResultSetMetaData getMetaData() { return null; }
        public void setDate(int parameterIndex, java.sql.Date x, java.util.Calendar cal) {}
        public void setTime(int parameterIndex, java.sql.Time x, java.util.Calendar cal) {}
        public void setTimestamp(int parameterIndex, java.sql.Timestamp x, java.util.Calendar cal) {}
        public void setNull(int parameterIndex, int sqlType, String typeName) {}
        public void setURL(int parameterIndex, java.net.URL x) {}
        public java.sql.ParameterMetaData getParameterMetaData() { return null; }
        public void setRowId(int parameterIndex, java.sql.RowId x) {}
        public void setNString(int parameterIndex, String value) {}
        public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length) {}
        public void setNClob(int parameterIndex, java.sql.NClob value) {}
        public void setClob(int parameterIndex, java.io.Reader reader, long length) {}
        public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length) {}
        public void setNClob(int parameterIndex, java.io.Reader reader, long length) {}
        public void setSQLXML(int parameterIndex, java.sql.SQLXML xmlObject) {}
        public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {}
        public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) {}
        public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) {}
        public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) {}
        public void setAsciiStream(int parameterIndex, java.io.InputStream x) {}
        public void setBinaryStream(int parameterIndex, java.io.InputStream x) {}
        public void setCharacterStream(int parameterIndex, java.io.Reader reader) {}
        public void setNCharacterStream(int parameterIndex, java.io.Reader value) {}
        public void setClob(int parameterIndex, java.io.Reader reader) {}
        public void setBlob(int parameterIndex, java.io.InputStream inputStream) {}
        public void setNClob(int parameterIndex, java.io.Reader reader) {}
        // Statement methods
        public java.sql.ResultSet executeQuery(String sql) { return null; }
        public int executeUpdate(String sql) { return 0; }
        public int getMaxFieldSize() { return 0; }
        public void setMaxFieldSize(int max) {}
        public int getMaxRows() { return 0; }
        public void setMaxRows(int max) {}
        public void setEscapeProcessing(boolean enable) {}
        public int getQueryTimeout() { return 0; }
        public void setQueryTimeout(int seconds) {}
        public void cancel() {}
        public java.sql.SQLWarning getWarnings() { return null; }
        public void clearWarnings() {}
        public void setCursorName(String name) {}
        public boolean execute(String sql) { return false; }
        public java.sql.ResultSet getResultSet() { return null; }
        public int getUpdateCount() { return -1; }
        public boolean getMoreResults() { return false; }
        public void setFetchDirection(int direction) {}
        public int getFetchDirection() { return 0; }
        public void setFetchSize(int rows) {}
        public int getFetchSize() { return 0; }
        public int getResultSetConcurrency() { return 0; }
        public int getResultSetType() { return 0; }
        public void addBatch(String sql) {}
        public void clearBatch() {}
        public int[] executeBatch() { return new int[0]; }
        public java.sql.Connection getConnection() { return null; }
        public boolean getMoreResults(int current) { return false; }
        public java.sql.ResultSet getGeneratedKeys() { return null; }
        public int executeUpdate(String sql, int autoGeneratedKeys) { return 0; }
        public int executeUpdate(String sql, int[] columnIndexes) { return 0; }
        public int executeUpdate(String sql, String[] columnNames) { return 0; }
        public boolean execute(String sql, int autoGeneratedKeys) { return false; }
        public boolean execute(String sql, int[] columnIndexes) { return false; }
        public boolean execute(String sql, String[] columnNames) { return false; }
        public int getResultSetHoldability() { return 0; }
        public boolean isClosed() { return false; }
        public void setPoolable(boolean poolable) {}
        public boolean isPoolable() { return false; }
        public void closeOnCompletion() {}
        public boolean isCloseOnCompletion() { return false; }
        public <T> T unwrap(Class<T> iface) { return null; }
        public boolean isWrapperFor(Class<?> iface) { return false; }
    }
}