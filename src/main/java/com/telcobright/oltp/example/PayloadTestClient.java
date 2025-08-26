package com.telcobright.oltp.example;

import com.telcobright.oltp.grpc.batch.BatchResponse;
import com.telcobright.oltp.grpc.builder.GrpcCrudPayloadBuilder;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.entity.PackageAccountReserve;
import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccountReserveDelta;
import com.telcobright.oltp.mapper.EntityProtoMapper;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

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
 * Enhanced test client that logs sent payloads and compares with server received logs
 */
public class PayloadTestClient {
    private static final Logger logger = LoggerFactory.getLogger(PayloadTestClient.class);
    private static final String CLIENT_LOG_FILE = "client-sent-payloads.log";
    private static final String SERVER_LOG_FILE = "server-received-payloads.log";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    private static final List<String> sentPayloads = new ArrayList<>();
    
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9000;
        
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        
        logger.info("╔══════════════════════════════════════════════════════════════╗");
        logger.info("║           Payload Test Client - Send & Verify                 ║");
        logger.info("║           Server: {}:{}                                  ║", host, port);
        logger.info("╚══════════════════════════════════════════════════════════════╝");
        
        try {
            // Clear previous logs
            clearLogFiles();
            
            // Execute all 3 test cases
            executeTestCase1(host, port);
            Thread.sleep(1000);
//
//            executeTestCase2(host, port);
//            Thread.sleep(1000);
//
//            executeTestCase3(host, port);
//            Thread.sleep(2000); // Wait for server to finish processing
            
            // Verify payloads
            boolean success = verifyPayloads();
            
            if (success) {
                logger.info("\n🎉 ALL TESTS PASSED! All payloads sent and received successfully.");
            } else {
                logger.error("\n❌ TESTS FAILED! Payload mismatch detected.");
                System.exit(1);
            }
            
        } catch (Exception e) {
            logger.error("❌ Test execution failed: ", e);
            System.exit(1);
        }
    }
    
    /**
     * Test Case 1: Update PackageAccount + Insert Reserve (op2 + op3)
     */
    private static void executeTestCase1(String host, int port) throws IOException {
        String testCase = "CASE1";
        String transactionId = testCase + "_" + UUID.randomUUID();
        
        logger.info("\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  TEST CASE 1: Update PackageAccount + Insert Reserve    │");
        logger.info("│  Transaction: {}                    │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // 1. Create strongly typed entities using new()
        PackageAccDelta accountDelta = new PackageAccDelta("telcobright", 199, new BigDecimal("5.50"));
        
        PackageAccountReserve reserve = new PackageAccountReserve();
        reserve.setId(4001L);
        reserve.setPackageAccountId(199L);
        reserve.setReservedAmount(new BigDecimal("30.00"));
        reserve.setSessionId("SESSION_CASE1_" + UUID.randomUUID());
        reserve.setStatus("RESERVED");
        
        // Log what we're about to send (including WAL format)
        String sentPayload = String.format(
            "SENT|%s|%s|telcobright|2|" +
            "OP1:UPDATE_DELTA|packageaccount|accountId=%d|amount=%s|" +
            "OP2:INSERT|packageaccountreserve|id=%d|packageAccountId=%d|reservedAmount=%s",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), transactionId,
            accountDelta.accountId, accountDelta.amount,
            reserve.getId(), reserve.getPackageAccountId(), reserve.getReservedAmount()
        );
        logClientPayload(sentPayload);
        sentPayloads.add(sentPayload);
        
        // 2. Convert entities to proto and send using GrpcCrudPayloadBuilder
        BatchResponse response = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("telcobright")
            .withTransactionId(transactionId)
            .addPackageAccountDelta(EntityProtoMapper.toProto(accountDelta))
            .addPackageAccountReserve(EntityProtoMapper.toProto(reserve))
            .submit();
        
        logger.info("✅ Case 1 Response: Success={}, Operations={}", 
            response.getSuccess(), response.getOperationsProcessed());
    }
    
    /**
     * Test Case 2: Update PackageAccount + Update Reserve (op2 + op4)
     */
    private static void executeTestCase2(String host, int port) throws IOException {
        String testCase = "CASE2";
        String transactionId = testCase + "_" + UUID.randomUUID();
        
        logger.info("\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  TEST CASE 2: Update PackageAccount + Update Reserve    │");
        logger.info("│  Transaction: {}                    │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // 1. Create strongly typed entities using new()
        PackageAccDelta accountDelta = new PackageAccDelta("res_1", 2001L, new BigDecimal("-25.75"));
        
        PackageAccountReserveDelta reserveDelta = new PackageAccountReserveDelta("res_1", 4001L, 
            new BigDecimal("-15.00"), "SESSION_CASE2_" + UUID.randomUUID());
        
        // Log what we're about to send (including WAL format)
        String sentPayload = String.format(
            "SENT|%s|%s|res_1|2|" +
            "OP1:UPDATE_DELTA|packageaccount|accountId=%d|amount=%s|" +
            "OP2:UPDATE_DELTA|packageaccountreserve|reserveId=%d|amount=%s",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), transactionId,
            accountDelta.accountId, accountDelta.amount,
            reserveDelta.reserveId, reserveDelta.amount
        );
        logClientPayload(sentPayload);
        sentPayloads.add(sentPayload);
        
        // 2. Convert entities to proto and send using GrpcCrudPayloadBuilder
        BatchResponse response = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .withTransactionId(transactionId)
            .addPackageAccountDelta(EntityProtoMapper.toProto(accountDelta))
            .addPackageAccountReserveDelta(EntityProtoMapper.toProto(reserveDelta))
            .submit();
        
        logger.info("✅ Case 2 Response: Success={}, Operations={}", 
            response.getSuccess(), response.getOperationsProcessed());
    }
    
    /**
     * Test Case 3: Delete PackageAccountReserve (op5)
     */
    private static void executeTestCase3(String host, int port) throws IOException {
        String testCase = "CASE3";
        String transactionId = testCase + "_" + UUID.randomUUID();
        
        logger.info("\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  TEST CASE 3: Delete PackageAccountReserve              │");
        logger.info("│  Transaction: {}                    │", transactionId);
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Log what we're about to send (including WAL format)
        String sentPayload = String.format(
            "SENT|%s|%s|res_1|1|" +
            "OP1:DELETE|packageaccountreserve|reserveId=5555",
            TIMESTAMP_FORMAT.format(LocalDateTime.now()), transactionId
        );
        logClientPayload(sentPayload);
        sentPayloads.add(sentPayload);
        
        // Generate WALEntries using GrpcCrudPayloadBuilder
        GrpcCrudPayloadBuilder builder = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .withTransactionId(transactionId)
            .deletePackageAccountReserve(5555);
        
        // Generate WALEntryBatch for WAL processing
        WALEntryBatch walBatch = builder.generateWALEntryBatch();
        logger.info("Generated WALEntryBatch with {} entries for transaction {}", walBatch.size(), transactionId);
        
        // Log WALEntryBatch details
        logger.info("WALEntryBatch: {}", walBatch.toString());
        for (int i = 0; i < walBatch.size(); i++) {
            WALEntry entry = walBatch.get(i);
            logger.info("WALEntry[{}]: db={}, table={}, op={}, data={}", 
                i, entry.getDbName(), entry.getTableName(), entry.getOperationType(), entry.getData());
        }
        
        // Submit original gRPC request
        BatchResponse response = builder.submit();
        
        logger.info("✅ Case 3 Response: Success={}, Operations={}", 
            response.getSuccess(), response.getOperationsProcessed());
    }
    
    /**
     * Log sent payload to client log file
     */
    private static void logClientPayload(String payload) throws IOException {
        try (FileWriter writer = new FileWriter(CLIENT_LOG_FILE, true)) {
            writer.write(payload + "\n");
            writer.flush();
        }
        logger.debug("📝 Logged sent payload: {}", payload);
    }
    
    /**
     * Verify that all sent payloads were received by the server
     */
    private static boolean verifyPayloads() throws IOException {
        logger.info("\n┌────────────────────────────────────────────────────────┐");
        logger.info("│  VERIFICATION: Comparing Sent vs Received Payloads      │");
        logger.info("└────────────────────────────────────────────────────────┘");
        
        // Wait for server log file to be created
        Path serverLogPath = Path.of(SERVER_LOG_FILE);
        int attempts = 0;
        while (!Files.exists(serverLogPath) && attempts < 10) {
            logger.info("⏳ Waiting for server log file... (attempt {})", attempts + 1);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            attempts++;
        }
        
        if (!Files.exists(serverLogPath)) {
            logger.error("❌ Server log file not found: {}", SERVER_LOG_FILE);
            return false;
        }
        
        // Read server received payloads
        List<String> serverPayloads = Files.readAllLines(serverLogPath);
        
        logger.info("📊 Sent Payloads: {}", sentPayloads.size());
        logger.info("📊 Received Payloads: {}", serverPayloads.size());
        
        if (sentPayloads.size() != serverPayloads.size()) {
            logger.error("❌ Payload count mismatch! Sent: {}, Received: {}", 
                sentPayloads.size(), serverPayloads.size());
            return false;
        }
        
        // Compare each payload
        boolean allMatch = true;
        for (int i = 0; i < sentPayloads.size(); i++) {
            String sent = sentPayloads.get(i);
            String received = i < serverPayloads.size() ? serverPayloads.get(i) : "";
            
            boolean matches = comparePayloads(sent, received);
            
            logger.info("🔍 Payload {} ({}): {}", 
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
     * Compare sent vs received payload (ignoring timestamps)
     */
    private static boolean comparePayloads(String sent, String received) {
        if (sent == null || received == null) {
            return false;
        }
        
        // Extract key parts (skip timestamps)
        String[] sentParts = sent.split("\\|");
        String[] receivedParts = received.split("\\|");
        
        if (sentParts.length < 4 || receivedParts.length < 4) {
            return false;
        }
        
        // Compare transaction ID, database, operation count, and operations
        return sentParts[2].equals(receivedParts[2]) &&  // Transaction ID
               sentParts[3].equals(receivedParts[3]) &&  // Database
               sentParts[4].equals(receivedParts[4]) &&  // Operation count
               compareOperations(sent, received);        // Operations
    }
    
    /**
     * Compare operation details
     */
    private static boolean compareOperations(String sent, String received) {
        // Extract operation parts after the 5th pipe
        String sentOps = sent.substring(sent.indexOf("|", sent.indexOf("|", sent.indexOf("|", sent.indexOf("|") + 1) + 1) + 1) + 1);
        String receivedOps = received.substring(received.indexOf("|", received.indexOf("|", received.indexOf("|", received.indexOf("|") + 1) + 1) + 1) + 1);
        
        return sentOps.equals(receivedOps);
    }
    
    /**
     * Extract test case from payload
     */
    private static String extractTestCase(String payload) {
        String[] parts = payload.split("\\|");
        if (parts.length >= 3) {
            String txnId = parts[2];
            if (txnId.startsWith("CASE1")) return "CASE1";
            if (txnId.startsWith("CASE2")) return "CASE2";
            if (txnId.startsWith("CASE3")) return "CASE3";
        }
        return "UNKNOWN";
    }
    
    /**
     * Clear previous log files
     */
    private static void clearLogFiles() throws IOException {
        Files.deleteIfExists(Path.of(CLIENT_LOG_FILE));
        Files.deleteIfExists(Path.of(SERVER_LOG_FILE));
        logger.info("🧹 Cleared previous log files");
    }
}