package com.telcobright.core.example;

import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Examples of how to populate WALEntryBatch with different operations
 * across multiple databases using the new transaction-level approach.
 */
public class WALEntryBatchExamples {
    private static final Logger logger = LoggerFactory.getLogger(WALEntryBatchExamples.class);
    
    public static void main(String[] args) {
        // Example 1: Single Database Operations
        demonstrateSingleDatabaseBatch();
        
        // Example 2: Multi-Database OP1 (Update Account + Insert Reserve)
        demonstrateMultiDatabaseOP1();
        
        // Example 3: Complex Multi-Operation Batch
        demonstrateComplexBatch();
    }
    
    /**
     * Example 1: Single Database Operations
     */
    public static void demonstrateSingleDatabaseBatch() {
        logger.info("=== Example 1: Single Database Operations ===");
        
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId("SINGLE_DB_TXN_" + UUID.randomUUID())
            .addEntry(createPackageAccountUpdate("telcobright", 1001L, "50.00"))
            .addEntry(createPackageAccountReserveInsert("telcobright", 4001L, 1001L, "25.00", "SESSION_001"))
            .build();
        
        logger.info("Created batch: {}", batch);
        logBatchDetails(batch);
    }
    
    /**
     * Example 2: Multi-Database OP1 (Update PackageAccount + Insert Reserve) across 3 databases
     */
    public static void demonstrateMultiDatabaseOP1() {
        logger.info("=== Example 2: Multi-Database OP1 ===");
        
        String globalTransactionId = "MULTI_DB_OP1_" + System.currentTimeMillis();
        
        // Database configurations
        DatabaseConfig[] databases = {
            new DatabaseConfig("telcobright", 1001L, 4001L, "50.00", "25.00"),
            new DatabaseConfig("res_1", 2001L, 4002L, "75.50", "30.00"),
            new DatabaseConfig("res_2", 3001L, 4003L, "100.25", "45.00")
        };
        
        WALEntryBatch.Builder batchBuilder = WALEntryBatch.builder()
            .transactionId(globalTransactionId);
        
        // Add OP1 (Update Account + Insert Reserve) for each database
        for (DatabaseConfig db : databases) {
            batchBuilder
                .addEntry(createPackageAccountUpdate(db.dbName, db.accountId, db.updateAmount))
                .addEntry(createPackageAccountReserveInsert(db.dbName, db.reserveId, db.accountId, db.reserveAmount, 
                    "SESSION_" + db.dbName.toUpperCase() + "_" + UUID.randomUUID()));
        }
        
        WALEntryBatch batch = batchBuilder.build();
        
        logger.info("Created multi-database batch: {}", batch);
        logger.info("Databases involved: {}", batch.getDatabaseNames());
        logBatchDetails(batch);
    }
    
    /**
     * Example 3: Complex Multi-Operation Batch
     */
    public static void demonstrateComplexBatch() {
        logger.info("=== Example 3: Complex Multi-Operation Batch ===");
        
        WALEntryBatch batch = WALEntryBatch.builder()
            .transactionId("COMPLEX_TXN_" + UUID.randomUUID())
            // Update operations
            .addEntry(createPackageAccountUpdate("telcobright", 1001L, "75.50"))
            .addEntry(createPackageAccountReserveUpdate("telcobright", 4001L, "-15.00", "SESSION_UPDATE"))
            // Insert operations
            .addEntry(createPackageAccountReserveInsert("res_1", 4002L, 2001L, "30.00", "SESSION_INSERT"))
            // Delete operations
            .addEntry(createPackageAccountReserveDelete("res_2", 5555L))
            // UPSERT operations
            .addEntry(createPackageAccountUpsert("telcobright", 3001L, "Data Package", "150.00", "GB"))
            .build();
        
        logger.info("Created complex batch: {}", batch);
        logBatchDetails(batch);
    }
    
    // ==================== WALEntry Factory Methods ====================
    
    /**
     * Create PackageAccount UPDATE operation
     */
    public static WALEntry createPackageAccountUpdate(String dbName, Long accountId, String amount) {
        return WALEntry.builder()
            .dbName(dbName)
            .tableName("packageaccount")
            .operationType(WALEntry.OperationType.UPDATE)
            .withData("accountId", accountId)
            .withData("amount", new BigDecimal(amount))
            .build();
    }
    
    /**
     * Create PackageAccountReserve INSERT operation
     */
    public static WALEntry createPackageAccountReserveInsert(String dbName, Long reserveId, Long accountId, 
                                                            String reserveAmount, String sessionId) {
        return WALEntry.builder()
            .dbName(dbName)
            .tableName("packageaccountreserve")
            .operationType(WALEntry.OperationType.INSERT)
            .withData("id", reserveId)
            .withData("packageAccountId", accountId)
            .withData("sessionId", sessionId)
            .withData("reservedAmount", new BigDecimal(reserveAmount))
            .withData("currentReserve", new BigDecimal(reserveAmount))
            .withData("status", "RESERVED")
            .withData("reservedAt", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))
            .build();
    }
    
    /**
     * Create PackageAccountReserve UPDATE operation
     */
    public static WALEntry createPackageAccountReserveUpdate(String dbName, Long reserveId, 
                                                           String deltaAmount, String sessionId) {
        return WALEntry.builder()
            .dbName(dbName)
            .tableName("packageaccountreserve")
            .operationType(WALEntry.OperationType.UPDATE)
            .withData("reserveId", reserveId)
            .withData("amount", new BigDecimal(deltaAmount))
            .withData("sessionId", sessionId)
            .build();
    }
    
    /**
     * Create PackageAccountReserve DELETE operation
     */
    public static WALEntry createPackageAccountReserveDelete(String dbName, Long reserveId) {
        return WALEntry.builder()
            .dbName(dbName)
            .tableName("packageaccountreserve")
            .operationType(WALEntry.OperationType.DELETE)
            .withData("reserveId", reserveId)
            .build();
    }
    
    /**
     * Create PackageAccount UPSERT operation
     */
    public static WALEntry createPackageAccountUpsert(String dbName, Long accountId, String name, 
                                                     String balanceAfter, String uom) {
        return WALEntry.builder()
            .dbName(dbName)
            .tableName("packageaccount")
            .operationType(WALEntry.OperationType.UPSERT)
            .withData("accountId", accountId)
            .withData("name", name)
            .withData("balanceAfter", new BigDecimal(balanceAfter))
            .withData("uom", uom)
            .build();
    }
    
    // ==================== Utility Methods ====================
    
    /**
     * Log detailed information about a WALEntryBatch
     */
    private static void logBatchDetails(WALEntryBatch batch) {
        logger.info("Transaction ID: {}", batch.getTransactionId());
        logger.info("Total entries: {}", batch.size());
        logger.info("Databases: {}", batch.getDatabaseNames());
        
        for (int i = 0; i < batch.size(); i++) {
            WALEntry entry = batch.get(i);
            logger.info("  Entry[{}]: {} {} on {}.{} - {}", 
                i, entry.getOperationType(), entry.getTableName(), 
                entry.getDbName(), entry.getTableName(), entry.getData());
        }
        logger.info("");
    }
    
    /**
     * Helper class for database configuration
     */
    private static class DatabaseConfig {
        String dbName;
        Long accountId;
        Long reserveId;
        String updateAmount;
        String reserveAmount;
        
        DatabaseConfig(String dbName, Long accountId, Long reserveId, String updateAmount, String reserveAmount) {
            this.dbName = dbName;
            this.accountId = accountId;
            this.reserveId = reserveId;
            this.updateAmount = updateAmount;
            this.reserveAmount = reserveAmount;
        }
    }
    
    // ==================== Pre-built Common Patterns ====================
    
    /**
     * Create OP1 pattern for multiple databases (Update Account + Insert Reserve)
     */
    public static WALEntryBatch createMultiDatabaseOP1(String transactionId, DatabaseConfig... databases) {
        WALEntryBatch.Builder builder = WALEntryBatch.builder().transactionId(transactionId);
        
        for (DatabaseConfig db : databases) {
            builder
                .addEntry(createPackageAccountUpdate(db.dbName, db.accountId, db.updateAmount))
                .addEntry(createPackageAccountReserveInsert(db.dbName, db.reserveId, db.accountId, 
                    db.reserveAmount, "SESSION_" + db.dbName.toUpperCase() + "_" + UUID.randomUUID()));
        }
        
        return builder.build();
    }
    
    /**
     * Create OP2 pattern (Update Account + Update Reserve)
     */
    public static WALEntryBatch createOP2Pattern(String transactionId, String dbName, 
                                                Long accountId, String accountDelta,
                                                Long reserveId, String reserveDelta, String sessionId) {
        return WALEntryBatch.builder()
            .transactionId(transactionId)
            .addEntry(createPackageAccountUpdate(dbName, accountId, accountDelta))
            .addEntry(createPackageAccountReserveUpdate(dbName, reserveId, reserveDelta, sessionId))
            .build();
    }
    
    /**
     * Create OP3 pattern (Delete Reserve)
     */
    public static WALEntryBatch createOP3Pattern(String transactionId, String dbName, Long reserveId) {
        return WALEntryBatch.builder()
            .transactionId(transactionId)
            .addEntry(createPackageAccountReserveDelete(dbName, reserveId))
            .build();
    }
}