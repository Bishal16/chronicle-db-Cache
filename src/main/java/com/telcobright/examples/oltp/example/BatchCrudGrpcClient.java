package com.telcobright.examples.oltp.example;

import com.telcobright.examples.oltp.grpc.batch.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Example gRPC client for testing batch CRUD operations
 */
public class BatchCrudGrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(BatchCrudGrpcClient.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    private final ManagedChannel channel;
    private final BatchCrudServiceGrpc.BatchCrudServiceBlockingStub blockingStub;
    
    public BatchCrudGrpcClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()  // For testing only
            .build();
        this.blockingStub = BatchCrudServiceGrpc.newBlockingStub(channel);
        
        logger.info("Connected to gRPC server at {}:{}", host, port);
    }
    
    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
    
    /**
     * Execute a batch of mixed operations
     */
    public void executeMixedBatch() {
        String transactionId = "TXN_" + UUID.randomUUID().toString();
        String dbName = "res_1";
        
        logger.info("========================================");
        logger.info("🚀 Sending mixed batch request");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("========================================");
        
        List<BatchOperation> operations = new ArrayList<>();
        
        // 1. Insert a new PackageAccount
        operations.add(createPackageAccountInsert());
        
        // 2. Update a PackageAccount using delta
        operations.add(createPackageAccountUpdateDelta());
        
        // 3. Insert a new PackageAccountReserve
        operations.add(createPackageAccountReserveInsert());
        
        // 4. Update a PackageAccountReserve using delta
        operations.add(createPackageAccountReserveUpdateDelta());
        
        // 5. Delete a PackageAccount using delta
        operations.add(createPackageAccountDeleteDelta());
        
        // 6. Delete a PackageAccountReserve using delta
        operations.add(createPackageAccountReserveDeleteDelta());
        
        // Build the batch request
        BatchRequest request = BatchRequest.newBuilder()
            .setTransactionId(transactionId)
            .setDbName(dbName)
            .addAllOperations(operations)
            .build();
        
        try {
            // Send the request
            logger.info("📤 Sending batch with {} operations...", operations.size());
            BatchResponse response = blockingStub.executeBatch(request);
            
            // Process the response
            logger.info("========================================");
            logger.info("📥 Received batch response");
            logger.info("Success: {}", response.getSuccess());
            logger.info("Message: {}", response.getMessage());
            logger.info("Transaction ID: {}", response.getTransactionId());
            logger.info("Operations Processed: {}", response.getOperationsProcessed());
            
            if (response.hasError()) {
                logger.error("Error Details:");
                logger.error("  Code: {}", response.getError().getCode());
                logger.error("  Message: {}", response.getError().getMessage());
                logger.error("  Failed at operation: {}", response.getError().getFailedOperationIndex());
            }
            
            logger.info("Individual Results:");
            for (OperationResult result : response.getResultsList()) {
                logger.info("  Operation {}: {} - {} on {} - {}", 
                    result.getOperationIndex(),
                    result.getSuccess() ? "✅" : "❌",
                    result.getOperationType(),
                    result.getEntityType(),
                    result.getMessage());
            }
            logger.info("========================================");
            
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {}", e.getStatus());
        }
    }
    
    private BatchOperation createPackageAccountInsert() {
        PackageAccount account = PackageAccount.newBuilder()
            .setId(1001)
            .setPackagePurchaseId(2001)
            .setName("Test Package Account")
            .setLastAmount("100.50")
            .setBalanceBefore("500.00")
            .setBalanceAfter("399.50")
            .setUom("MB")
            .setIsSelected(true)
            .build();
        
        return BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccount(account)
            .build();
    }
    
    private BatchOperation createPackageAccountUpdateDelta() {
        PackageAccountDelta delta = PackageAccountDelta.newBuilder()
            .setDbName("res_1")
            .setAccountId(1001)
            .setAmount("50.25")
            .build();
        
        return BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDelta(delta)
            .build();
    }
    
    private BatchOperation createPackageAccountReserveInsert() {
        PackageAccountReserve reserve = PackageAccountReserve.newBuilder()
            .setId(3001)
            .setPackageAccountId(1001)
            .setSessionId("SESSION_" + UUID.randomUUID().toString())
            .setReservedAmount("25.00")
            .setReservedAt(LocalDateTime.now().format(DATE_FORMATTER))
            .setStatus("RESERVED")
            .setCurrentReserve("25.00")
            .build();
        
        return BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserve(reserve)
            .build();
    }
    
    private BatchOperation createPackageAccountReserveUpdateDelta() {
        PackageAccountReserveDelta delta = PackageAccountReserveDelta.newBuilder()
            .setDbName("res_1")
            .setReserveId(3001)
            .setAmount("-10.00")  // Negative for release
            .setSessionId("SESSION_" + UUID.randomUUID().toString())
            .build();
        
        return BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDelta(delta)
            .build();
    }
    
    private BatchOperation createPackageAccountDeleteDelta() {
        PackageAccountDeleteDelta delta = PackageAccountDeleteDelta.newBuilder()
            .setDbName("res_1")
            .setAccountId(999)
            .build();
        
        return BatchOperation.newBuilder()
            .setOperationType(OperationType.DELETE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDeleteDelta(delta)
            .build();
    }
    
    private BatchOperation createPackageAccountReserveDeleteDelta() {
        PackageAccountReserveDeleteDelta delta = PackageAccountReserveDeleteDelta.newBuilder()
            .setDbName("res_1")
            .setReserveId(888)
            .build();
        
        return BatchOperation.newBuilder()
            .setOperationType(OperationType.DELETE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDeleteDelta(delta)
            .build();
    }
    
    /**
     * Test with a failing batch (to test rollback)
     */
    public void executeFailingBatch() {
        String transactionId = "TXN_FAIL_" + UUID.randomUUID().toString();
        String dbName = "res_1";
        
        logger.info("========================================");
        logger.info("🔴 Sending batch that should fail");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("========================================");
        
        List<BatchOperation> operations = new ArrayList<>();
        
        // Add a valid operation
        operations.add(createPackageAccountInsert());
        
        // Add an invalid operation (empty payload)
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            // No payload set - this should cause an error
            .build());
        
        BatchRequest request = BatchRequest.newBuilder()
            .setTransactionId(transactionId)
            .setDbName(dbName)
            .addAllOperations(operations)
            .build();
        
        try {
            BatchResponse response = blockingStub.executeBatch(request);
            
            logger.info("Response Success: {}", response.getSuccess());
            logger.info("Response Message: {}", response.getMessage());
            
            if (!response.getSuccess()) {
                logger.info("✅ Batch failed as expected - transaction was rolled back");
            }
            
        } catch (StatusRuntimeException e) {
            logger.error("RPC failed: {}", e.getStatus());
        }
    }
    
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9000;  // Default Quarkus gRPC port
        
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        
        BatchCrudGrpcClient client = new BatchCrudGrpcClient(host, port);
        
        try {
            // Test 1: Execute a mixed batch of operations
            client.executeMixedBatch();
            
            Thread.sleep(2000);
            
            // Test 2: Execute a failing batch to test rollback
            client.executeFailingBatch();
            
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