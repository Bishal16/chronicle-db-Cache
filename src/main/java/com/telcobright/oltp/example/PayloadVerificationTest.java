package com.telcobright.oltp.example;

import com.telcobright.oltp.grpc.builder.WALBatchGrpcClient;
import com.telcobright.oltp.grpc.wal.WALBatchResponse;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Client-side test to verify exact payload matching between client and server
 * Constructs WALEntryBatch -> Sends to gRPC -> Logs serialized payload -> Verifies match
 */
public class PayloadVerificationTest {
    private static final Logger logger = LoggerFactory.getLogger(PayloadVerificationTest.class);
    private static final String CLIENT_LOG_FILE = "client-sent-wal-batches.log";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    public static void main(String[] args) {
        logger.info("╔════════════════════════════════════════════════════════════════╗");
        logger.info("║              WALEntryBatch Payload Verification                ║");
        logger.info("║        Client->gRPC->Server Payload Matching Test             ║");
        logger.info("╚════════════════════════════════════════════════════════════════╝");
        
        try {
            // Test Case 1: Single Database Operations
            runTestCase1();
            
            // Test Case 2: Multi-Database Operations (Your Original OP1 Pattern)  
            runTestCase2();
            
            // Test Case 3: Delete Operations
            runTestCase3();
            
            logger.info("✅ All test cases completed!");
            logger.info("📝 Client logs written to: {}", CLIENT_LOG_FILE);
            logger.info("📝 Server logs should be in: server-received-wal-batches.log");
            logger.info("🔍 Use PayloadLogComparator to verify exact matching!");
            
        } catch (Exception e) {
            logger.error("❌ Test failed", e);
            System.exit(1);
        }
    }
    
    /**
     * Test Case 1: Single Database Operations
     */
    private static void runTestCase1() {
        logger.info("🧪 TEST CASE 1: Single Database Operations");
        logger.info("══════════════════════════════════════════");
        
        try {
            String txnId = "CASE1_" + UUID.randomUUID().toString().substring(0, 8);
            
            // Construct WALEntryBatch
            WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
                .transactionId(txnId)
                .updatePackageAccount("telcobright", 1001L, "75.50")
                .insertPackageAccountReserve("telcobright", 4001L, 1001L, "30.00", "SESSION_001")
                .build();
            
            logger.info("📦 Constructed WALEntryBatch:");
            logger.info("   Transaction ID: {}", walBatch.getTransactionId());
            logger.info("   Entries: {}", walBatch.size());
            logger.info("   Databases: {}", walBatch.getDatabaseNames());
            
            // Log serialized payload BEFORE sending
            logSentWALBatch(walBatch);
            
            // Send to server
            WALBatchGrpcClient client = WALBatchGrpcClient.create("localhost", 9000);
            WALBatchResponse response = client.sendWALBatch(walBatch);
            
            logger.info("✅ TEST CASE 1 Response: Success={}, Entries={}", 
                response.getSuccess(), response.getEntriesProcessed());
            
        } catch (Exception e) {
            logger.error("❌ TEST CASE 1 Failed", e);
        }
    }
    
    /**
     * Test Case 2: Multi-Database Operations (OP1 Pattern)
     */
    private static void runTestCase2() {
        logger.info("🧪 TEST CASE 2: Multi-Database OP1 Pattern");
        logger.info("══════════════════════════════════════════");
        
        try {
            String txnId = "MULTI_" + UUID.randomUUID().toString().substring(0, 8);
            
            // Construct your original multi-database OP1 pattern
            WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
                .transactionId(txnId)
                // Database 1: telcobright
                .updatePackageAccount("telcobright", 1001L, "50.00")
                .insertPackageAccountReserve("telcobright", 4001L, 1001L, "25.00", "SESSION_TB")
                // Database 2: res_1
                .updatePackageAccount("res_1", 2001L, "75.50")
                .insertPackageAccountReserve("res_1", 4002L, 2001L, "30.00", "SESSION_R1")
                // Database 3: res_2
                .updatePackageAccount("res_2", 3001L, "100.25")
                .insertPackageAccountReserve("res_2", 4003L, 3001L, "45.00", "SESSION_R2")
                .build();
            
            logger.info("📦 Constructed Multi-DB WALEntryBatch:");
            logger.info("   Transaction ID: {}", walBatch.getTransactionId());
            logger.info("   Entries: {}", walBatch.size());
            logger.info("   Databases: {}", walBatch.getDatabaseNames());
            
            // Log each entry details
            for (int i = 0; i < walBatch.size(); i++) {
                WALEntry entry = walBatch.get(i);
                logger.info("   Entry[{}]: {} on {}.{} - {}", 
                    i, entry.getOperationType(), entry.getDbName(), 
                    entry.getTableName(), entry.getData());
            }
            
            // Log serialized payload BEFORE sending
            logSentWALBatch(walBatch);
            
            // Send to server
            WALBatchGrpcClient client = WALBatchGrpcClient.create("localhost", 9000);
            WALBatchResponse response = client.sendWALBatch(walBatch);
            
            logger.info("✅ TEST CASE 2 Response: Success={}, Entries={}", 
                response.getSuccess(), response.getEntriesProcessed());
                
        } catch (Exception e) {
            logger.error("❌ TEST CASE 2 Failed", e);
        }
    }
    
    /**
     * Test Case 3: Delete Operations
     */
    private static void runTestCase3() {
        logger.info("🧪 TEST CASE 3: Delete Operations");
        logger.info("═══════════════════════════════════");
        
        try {
            String txnId = "DEL_" + UUID.randomUUID().toString().substring(0, 8);
            
            // Construct delete batch
            WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
                .transactionId(txnId)
                .deletePackageAccountReserve("res_1", 5555L)
                .build();
            
            logger.info("📦 Constructed Delete WALEntryBatch:");
            logger.info("   Transaction ID: {}", walBatch.getTransactionId());
            logger.info("   Entries: {}", walBatch.size());
            logger.info("   Operation: DELETE");
            
            // Log serialized payload BEFORE sending
            logSentWALBatch(walBatch);
            
            // Send to server
            WALBatchGrpcClient client = WALBatchGrpcClient.create("localhost", 9000);
            WALBatchResponse response = client.sendWALBatch(walBatch);
            
            logger.info("✅ TEST CASE 3 Response: Success={}, Entries={}", 
                response.getSuccess(), response.getEntriesProcessed());
                
        } catch (Exception e) {
            logger.error("❌ TEST CASE 3 Failed", e);
        }
    }
    
    /**
     * Log sent WALBatch to file in the same format as server for exact comparison
     */
    private static void logSentWALBatch(WALEntryBatch walBatch) {
        try {
            StringBuilder batchLog = new StringBuilder();
            batchLog.append("SENT_WAL_BATCH|");
            batchLog.append(TIMESTAMP_FORMAT.format(LocalDateTime.now())).append("|");
            batchLog.append(walBatch.getTransactionId()).append("|");
            batchLog.append(walBatch.size()).append("|");
            
            // Get all database names
            List<String> databases = walBatch.getDatabaseNames();
            batchLog.append("databases=").append(String.join(",", databases)).append("|");
            
            // Log each entry in EXACT same format as server
            for (int i = 0; i < walBatch.size(); i++) {
                WALEntry entry = walBatch.get(i);
                batchLog.append("ENTRY").append(i + 1).append(":");
                batchLog.append(entry.getOperationType()).append("|");
                batchLog.append(entry.getDbName()).append(".").append(entry.getTableName()).append("|");
                batchLog.append(entry.getData().toString());
                
                if (i < walBatch.size() - 1) {
                    batchLog.append("|");
                }
            }
            
            // Write to client log file
            try (FileWriter writer = new FileWriter(CLIENT_LOG_FILE, true)) {
                writer.write(batchLog.toString() + "\n");
                writer.flush();
            }
            
            logger.info("📝 Logged SENT WALBatch to: {}", CLIENT_LOG_FILE);
            
        } catch (IOException e) {
            logger.error("Failed to log sent WALBatch", e);
        }
    }
}