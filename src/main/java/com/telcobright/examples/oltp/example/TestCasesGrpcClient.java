package com.telcobright.examples.oltp.example;

import com.telcobright.examples.oltp.grpc.batch.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Test client with specific test cases for batch operations
 */
public class TestCasesGrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(TestCasesGrpcClient.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    private final ManagedChannel channel;
    private final BatchCrudServiceGrpc.BatchCrudServiceBlockingStub blockingStub;
    
    public TestCasesGrpcClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build();
        this.blockingStub = BatchCrudServiceGrpc.newBlockingStub(channel);
        
        logger.info("Connected to gRPC server at {}:{}", host, port);
    }
    
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    /**
     * Case 1: Update PackageAccount with op2 + op3
     * - op2: PackageAccountDelta (UPDATE_DELTA)
     * - op3: PackageAccountReserveInsert (INSERT)
     */
    public void executeTestCase1() {
        String transactionId = "CASE1_" + UUID.randomUUID().toString();
        String dbName = "res_1";
        
        logger.info("\n========================================");
        logger.info("📋 TEST CASE 1: Update PackageAccount");
        logger.info("Operations: PackageAccountDelta + PackageAccountReserve Insert");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("========================================");
        
        List<BatchOperation> operations = new ArrayList<>();
        
        // op2: PackageAccountDelta (UPDATE_DELTA)
        PackageAccountDelta accountDelta = PackageAccountDelta.newBuilder()
            .setDbName(dbName)
            .setAccountId(1001)
            .setAmount("75.50")
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDelta(accountDelta)
            .build());
        
        // op3: PackageAccountReserve Insert
        PackageAccountReserve reserve = PackageAccountReserve.newBuilder()
            .setId(4001)
            .setPackageAccountId(1001)
            .setSessionId("SESSION_CASE1_" + UUID.randomUUID().toString())
            .setReservedAmount("30.00")
            .setReservedAt(LocalDateTime.now().format(DATE_FORMATTER))
            .setStatus("ACTIVE")
            .setCurrentReserve("30.00")
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserve(reserve)
            .build());
        
        sendBatchRequest(transactionId, dbName, operations, "CASE 1");
    }
    
    /**
     * Case 2: Update PackageAccount with op2 + op4
     * - op2: PackageAccountDelta (UPDATE_DELTA)
     * - op4: PackageAccountReserveDelta (UPDATE_DELTA)
     */
    public void executeTestCase2() {
        String transactionId = "CASE2_" + UUID.randomUUID().toString();
        String dbName = "res_1";
        
        logger.info("\n========================================");
        logger.info("📋 TEST CASE 2: Update PackageAccount");
        logger.info("Operations: PackageAccountDelta + PackageAccountReserveDelta");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("========================================");
        
        List<BatchOperation> operations = new ArrayList<>();
        
        // op2: PackageAccountDelta (UPDATE_DELTA)
        PackageAccountDelta accountDelta = PackageAccountDelta.newBuilder()
            .setDbName(dbName)
            .setAccountId(2001)
            .setAmount("-25.75")
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDelta(accountDelta)
            .build());
        
        // op4: PackageAccountReserveDelta (UPDATE_DELTA)
        PackageAccountReserveDelta reserveDelta = PackageAccountReserveDelta.newBuilder()
            .setDbName(dbName)
            .setReserveId(4001)
            .setAmount("-15.00")  // Release reserve
            .setSessionId("SESSION_CASE2_" + UUID.randomUUID().toString())
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDelta(reserveDelta)
            .build());
        
        sendBatchRequest(transactionId, dbName, operations, "CASE 2");
    }
    
    /**
     * Case 3: Delete PackageAccountReserve with op5
     * - op5: PackageAccountReserveDeleteDelta (DELETE_DELTA)
     */
    public void executeTestCase3() {
        String transactionId = "CASE3_" + UUID.randomUUID().toString();
        String dbName = "res_1";
        
        logger.info("\n========================================");
        logger.info("📋 TEST CASE 3: Delete PackageAccountReserve");
        logger.info("Operations: PackageAccountReserveDeleteDelta");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("========================================");
        
        List<BatchOperation> operations = new ArrayList<>();
        
        // op5: PackageAccountReserveDeleteDelta (DELETE_DELTA)
        PackageAccountReserveDeleteDelta deleteDelta = PackageAccountReserveDeleteDelta.newBuilder()
            .setDbName(dbName)
            .setReserveId(5555)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.DELETE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDeleteDelta(deleteDelta)
            .build());
        
        sendBatchRequest(transactionId, dbName, operations, "CASE 3");
    }
    
    private void sendBatchRequest(String transactionId, String dbName, 
                                 List<BatchOperation> operations, String caseLabel) {
        BatchRequest request = BatchRequest.newBuilder()
            .setTransactionId(transactionId)
            .setDbName(dbName)
            .addAllOperations(operations)
            .build();
        
        try {
            logger.info("📤 Sending {} with {} operations...", caseLabel, operations.size());
            
            BatchResponse response = blockingStub.executeBatch(request);
            
            logger.info("📥 Response for {}", caseLabel);
            logger.info("  Success: {}", response.getSuccess());
            logger.info("  Message: {}", response.getMessage());
            logger.info("  Operations Processed: {}", response.getOperationsProcessed());
            
            if (response.hasError()) {
                logger.error("  ❌ Error: {}", response.getError().getMessage());
            }
            
            for (OperationResult result : response.getResultsList()) {
                logger.info("  {} Operation {}: {} - {}", 
                    result.getSuccess() ? "✅" : "❌",
                    result.getOperationIndex(),
                    result.getOperationType(),
                    result.getEntityType());
            }
            
        } catch (StatusRuntimeException e) {
            logger.error("❌ RPC failed for {}: {}", caseLabel, e.getStatus());
        }
    }
    
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9000;
        
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        
        TestCasesGrpcClient client = new TestCasesGrpcClient(host, port);
        
        try {
            // Execute all test cases with delay between them
            client.executeTestCase1();
            Thread.sleep(2000);
            
            client.executeTestCase2();
            Thread.sleep(2000);
            
            client.executeTestCase3();
            
            logger.info("\n========================================");
            logger.info("✅ All test cases completed");
            logger.info("========================================");
            
        } catch (Exception e) {
            logger.error("Error during client execution: ", e);
        } finally {
            try {
                client.shutdown();
                logger.info("Client shutdown complete");
            } catch (InterruptedException e) {
                logger.error("Error during shutdown: ", e);
            }
        }
    }
}