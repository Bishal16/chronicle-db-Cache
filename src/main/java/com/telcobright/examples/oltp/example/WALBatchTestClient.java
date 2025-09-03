package com.telcobright.examples.oltp.example;

import com.telcobright.examples.oltp.grpc.builder.WALBatchGrpcClient;
import com.telcobright.examples.oltp.grpc.wal.WALBatchResponse;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Test client for WALEntryBatch gRPC transmission
 * Tests atomic WALEntryBatch operations across multiple databases
 */
public class WALBatchTestClient {
    private static final Logger logger = LoggerFactory.getLogger(WALBatchTestClient.class);
    private static final String CLIENT_LOG_FILE = "client-sent-wal-batches.log";
    private static final String SERVER_LOG_FILE = "server-received-wal-batches.log";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    private static final List<String> sentBatches = new ArrayList<>();
    
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9000;
        
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        
        logger.info("╔══════════════════════════════════════════════════════════════╗");
        logger.info("║           WALBatch Test Client - Atomic Operations           ║");
        logger.info("║           Server: {}:{}                                  ║", host, port);
        logger.info("╚══════════════════════════════════════════════════════════════╝");
        
        try {
            // Clear previous logs
            clearLogFiles();
            
            // Execute all 3 test cases with WALEntryBatch
            executeTestCase1(host, port);
            Thread.sleep(1000);
            
            executeTestCase2(host, port);
            Thread.sleep(1000);
            
            executeTestCase3(host, port);
            Thread.sleep(1000);
            
            // Execute multi-database test
            executeMultiDatabaseTest(host, port);
            Thread.sleep(2000); // Wait for server to finish processing
            
            // Verify batches
            boolean success = verifyWALBatches();
            
            if (success) {
                logger.info("\\n🎉 ALL WAL BATCH TESTS PASSED! All batches sent and received successfully.");
            } else {
                logger.error("\\n❌ WAL BATCH TESTS FAILED! Batch mismatch detected.");
                System.exit(1);
            }
            
        } catch (Exception e) {
            logger.error("❌ Test execution failed: ", e);
            System.exit(1);
        }
    }
    
    /**
     * Test Case 1: Update PackageAccount + Insert Reserve (Single Database)
     */
    private static void executeTestCase1(String host, int port) throws IOException {
        String transactionId = "WAL_CASE1_" + UUID.randomUUID();
        
        logger.info("\\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  WAL TEST CASE 1: Update Account + Insert Reserve      │");
        logger.info("│  Transaction: {}                  │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Create WALEntryBatch using builder
        WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId(transactionId)
            .updatePackageAccount("telcobright", 1001L, "75.50")
            .insertPackageAccountReserve("telcobright", 4001L, 1001L, "30.00", "SESSION_CASE1_" + UUID.randomUUID())
            .build();
        
        // Log what we're about to send
        String sentPayload = String.format(
            "SENT_WAL_BATCH|%s|%s|%d|databases=%s|entries=%s",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), 
            transactionId,
            walBatch.size(),
            String.join(",", walBatch.getDatabaseNames()),
            walBatch.toString()
        );
        logClientBatch(sentPayload);
        sentBatches.add(sentPayload);
        
        // Send WALEntryBatch via gRPC
        WALBatchGrpcClient client = WALBatchGrpcClient.create(host, port);
        WALBatchResponse response = client.sendWALBatch(walBatch);
        
        logger.info("✅ Case 1 Response: Success={}, Entries Processed={}, WAL Index={}", 
            response.getSuccess(), response.getEntriesProcessed(), response.getWalIndex());
    }
    
    /**
     * Test Case 2: Update PackageAccount + Update Reserve (Single Database)
     */
    private static void executeTestCase2(String host, int port) throws IOException {
        String transactionId = "WAL_CASE2_" + UUID.randomUUID();
        
        logger.info("\\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  WAL TEST CASE 2: Update Account + Update Reserve      │");
        logger.info("│  Transaction: {}                  │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Create WALEntryBatch using builder
        WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId(transactionId)
            .updatePackageAccount("res_1", 2001L, "-25.75")
            .updatePackageAccountReserve("res_1", 4001L, "-15.00", "SESSION_CASE2_" + UUID.randomUUID())
            .build();
        
        // Log what we're about to send
        String sentPayload = String.format(
            "SENT_WAL_BATCH|%s|%s|%d|databases=%s|entries=%s",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), 
            transactionId,
            walBatch.size(),
            String.join(",", walBatch.getDatabaseNames()),
            walBatch.toString()
        );
        logClientBatch(sentPayload);
        sentBatches.add(sentPayload);
        
        // Send WALEntryBatch via gRPC
        WALBatchGrpcClient client = WALBatchGrpcClient.create(host, port);
        WALBatchResponse response = client.sendWALBatch(walBatch);
        
        logger.info("✅ Case 2 Response: Success={}, Entries Processed={}, WAL Index={}", 
            response.getSuccess(), response.getEntriesProcessed(), response.getWalIndex());
    }
    
    /**
     * Test Case 3: Delete PackageAccountReserve
     */
    private static void executeTestCase3(String host, int port) throws IOException {
        String transactionId = "WAL_CASE3_" + UUID.randomUUID();
        
        logger.info("\\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  WAL TEST CASE 3: Delete PackageAccountReserve         │");
        logger.info("│  Transaction: {}                  │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Create WALEntryBatch using builder
        WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId(transactionId)
            .deletePackageAccountReserve("res_1", 5555L)
            .build();
        
        // Log what we're about to send
        String sentPayload = String.format(
            "SENT_WAL_BATCH|%s|%s|%d|databases=%s|entries=%s",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), 
            transactionId,
            walBatch.size(),
            String.join(",", walBatch.getDatabaseNames()),
            walBatch.toString()
        );
        logClientBatch(sentPayload);
        sentBatches.add(sentPayload);
        
        // Send WALEntryBatch via gRPC
        WALBatchGrpcClient client = WALBatchGrpcClient.create(host, port);
        WALBatchResponse response = client.sendWALBatch(walBatch);
        
        logger.info("✅ Case 3 Response: Success={}, Entries Processed={}, WAL Index={}", 
            response.getSuccess(), response.getEntriesProcessed(), response.getWalIndex());
    }
    
    /**
     * Test Case 4: Multi-Database OP1 (Update Account + Insert Reserve across 3 databases)
     */
    private static void executeMultiDatabaseTest(String host, int port) throws IOException {
        String transactionId = "WAL_MULTI_DB_" + UUID.randomUUID();
        
        logger.info("\\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  WAL MULTI-DB TEST: OP1 Across 3 Databases             │");
        logger.info("│  Transaction: {}                │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Create multi-database WALEntryBatch
        WALEntryBatch walBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId(transactionId)
            // Database 1: telcobright
            .updatePackageAccount("telcobright", 1001L, "50.00")
            .insertPackageAccountReserve("telcobright", 4001L, 1001L, "25.00", "SESSION_TB_" + UUID.randomUUID())
            // Database 2: res_1
            .updatePackageAccount("res_1", 2001L, "75.50")
            .insertPackageAccountReserve("res_1", 4002L, 2001L, "30.00", "SESSION_R1_" + UUID.randomUUID())
            // Database 3: res_2
            .updatePackageAccount("res_2", 3001L, "100.25")
            .insertPackageAccountReserve("res_2", 4003L, 3001L, "45.00", "SESSION_R2_" + UUID.randomUUID())
            .build();
        
        logger.info("Created multi-database WALBatch: {}", walBatch.toString());
        
        // Log what we're about to send
        String sentPayload = String.format(
            "SENT_WAL_BATCH|%s|%s|%d|databases=%s|entries=%s",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), 
            transactionId,
            walBatch.size(),
            String.join(",", walBatch.getDatabaseNames()),
            walBatch.toString()
        );
        logClientBatch(sentPayload);
        sentBatches.add(sentPayload);
        
        // Send WALEntryBatch via gRPC
        WALBatchGrpcClient client = WALBatchGrpcClient.create(host, port);
        WALBatchResponse response = client.sendWALBatch(walBatch);
        
        logger.info("✅ Multi-DB Response: Success={}, Entries Processed={}, WAL Index={}", 
            response.getSuccess(), response.getEntriesProcessed(), response.getWalIndex());
        logger.info("   Databases processed: {}", walBatch.getDatabaseNames());
    }
    
    /**
     * Log sent WAL batch to client log file
     */
    private static void logClientBatch(String payload) throws IOException {
        try (FileWriter writer = new FileWriter(CLIENT_LOG_FILE, true)) {
            writer.write(payload + "\\n");
            writer.flush();
        }
        logger.debug("📝 Logged sent WAL batch: {}", payload);
    }
    
    /**
     * Verify that all sent WAL batches were received by the server
     */
    private static boolean verifyWALBatches() throws IOException {
        logger.info("\\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  VERIFICATION: Comparing Sent vs Received WAL Batches   │");
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Wait for server log file to be created
        Path serverLogPath = Path.of(SERVER_LOG_FILE);
        int attempts = 0;
        while (!Files.exists(serverLogPath) && attempts < 10) {
            logger.info("⏳ Waiting for server WAL batch log file... (attempt {})", attempts + 1);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            attempts++;
        }
        
        if (!Files.exists(serverLogPath)) {
            logger.error("❌ Server WAL batch log file not found: {}", SERVER_LOG_FILE);
            return false;
        }
        
        // Read server received batches
        List<String> serverBatches = Files.readAllLines(serverLogPath);
        
        logger.info("📊 Sent WAL Batches: {}", sentBatches.size());
        logger.info("📊 Received WAL Batches: {}", serverBatches.size());
        
        if (sentBatches.size() != serverBatches.size()) {
            logger.error("❌ WAL batch count mismatch! Sent: {}, Received: {}", 
                sentBatches.size(), serverBatches.size());
            return false;
        }
        
        // Compare each batch (simplified comparison)
        boolean allMatch = true;
        for (int i = 0; i < sentBatches.size(); i++) {
            String sent = sentBatches.get(i);
            String received = i < serverBatches.size() ? serverBatches.get(i) : "";
            
            boolean matches = compareWALBatches(sent, received);
            
            logger.info("🔍 WAL Batch {} ({}): {}", 
                i + 1, 
                extractTestCase(sent),
                matches ? "✅ MATCH" : "❌ MISMATCH");
            
            if (!matches) {
                logger.error("   📤 SENT:     {}", sent);
                logger.error("   📥 RECEIVED: {}", received);
                allMatch = false;
            }
        }
        
        return allMatch;
    }
    
    /**
     * Compare sent vs received WAL batch (simplified comparison)
     */
    private static boolean compareWALBatches(String sent, String received) {
        if (sent == null || received == null) {
            return false;
        }
        
        // Extract key parts (skip timestamps)
        String[] sentParts = sent.split("\\\\|");
        String[] receivedParts = received.split("\\\\|");
        
        if (sentParts.length < 3 || receivedParts.length < 3) {
            return false;
        }
        
        // Compare transaction ID and entry count (simplified)
        return sentParts[2].equals(receivedParts[2]) &&  // Transaction ID
               sentParts[3].equals(receivedParts[3]);     // Entry count
    }
    
    /**
     * Extract test case from batch payload
     */
    private static String extractTestCase(String payload) {
        if (payload.contains("WAL_CASE1")) return "CASE1";
        if (payload.contains("WAL_CASE2")) return "CASE2";
        if (payload.contains("WAL_CASE3")) return "CASE3";
        if (payload.contains("WAL_MULTI_DB")) return "MULTI_DB";
        return "UNKNOWN";
    }
    
    /**
     * Clear previous log files
     */
    private static void clearLogFiles() throws IOException {
        Files.deleteIfExists(Path.of(CLIENT_LOG_FILE));
        Files.deleteIfExists(Path.of(SERVER_LOG_FILE));
        logger.info("🧹 Cleared previous WAL batch log files");
    }
}