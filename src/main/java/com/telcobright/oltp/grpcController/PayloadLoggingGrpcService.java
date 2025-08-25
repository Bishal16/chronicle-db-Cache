package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.grpc.batch.*;
import com.telcobright.oltp.dbCache.CacheManager;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Enhanced gRPC service that logs all received payloads to file for verification
 */
// @GrpcService  // Disabled - using embedded server in PayloadTestServer
public class PayloadLoggingGrpcService extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(PayloadLoggingGrpcService.class);
    private static final String SERVER_LOG_FILE = "server-received-payloads.log";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    
    @Inject
    CacheManager cacheManager;
    
    @Override
    public void executeBatch(BatchRequest request, StreamObserver<BatchResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        String dbName = request.getDbName();
        List<BatchOperation> operations = request.getOperationsList();
        
        logger.info("════════════════════════════════════════════════════════");
        logger.info("📥 Payload Logging Service - Batch Request Received");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("Operations count: {}", operations.size());
        logger.info("════════════════════════════════════════════════════════");
        
        try {
            // Log the received payload to file
            logReceivedPayload(transactionId, dbName, operations);
            
            // Process operations (same as GrpcServiceGenericCrud but with logging)
            List<OperationResult> results = new ArrayList<>();
            boolean allSuccess = true;
            
            for (int i = 0; i < operations.size(); i++) {
                BatchOperation operation = operations.get(i);
                logger.info("🔄 Processing Operation #{}: {} {}", 
                    i + 1, operation.getOperationType(), operation.getEntityType());
                
                try {
                    OperationResult result = processOperation(operation, dbName, transactionId, i);
                    results.add(result);
                    
                    if (!result.getSuccess()) {
                        allSuccess = false;
                        logger.error("❌ Operation {} failed: {}", i, result.getMessage());
                    } else {
                        logger.info("✅ Operation {} completed successfully", i + 1);
                    }
                } catch (Exception e) {
                    logger.error("❌ Error processing operation {}: ", i, e);
                    allSuccess = false;
                    
                    results.add(OperationResult.newBuilder()
                        .setOperationIndex(i)
                        .setSuccess(false)
                        .setMessage("Error: " + e.getMessage())
                        .setEntityType(operation.getEntityType())
                        .setOperationType(operation.getOperationType())
                        .build());
                }
            }
            
            logger.info("════════════════════════════════════════════════════════");
            logger.info("{} Payload logged and processed", allSuccess ? "✅" : "⚠️");
            logger.info("Success: {}, Processed: {}/{}", allSuccess, results.size(), operations.size());
            logger.info("════════════════════════════════════════════════════════");
            
            // Build response
            BatchResponse.Builder responseBuilder = BatchResponse.newBuilder()
                .setSuccess(allSuccess)
                .setTransactionId(transactionId)
                .setOperationsProcessed(results.size())
                .addAllResults(results);
            
            if (allSuccess) {
                responseBuilder.setMessage("All operations received, logged, and processed successfully");
            } else {
                responseBuilder.setMessage("Some operations failed - check logs")
                    .setError(ErrorDetails.newBuilder()
                        .setCode("PARTIAL_FAILURE")
                        .setMessage("Check individual operation results")
                        .build());
            }
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("❌ Critical error in payload processing: ", e);
            
            BatchResponse errorResponse = BatchResponse.newBuilder()
                .setSuccess(false)
                .setTransactionId(transactionId)
                .setOperationsProcessed(0)
                .setMessage("Critical server error")
                .setError(ErrorDetails.newBuilder()
                    .setCode("SERVER_ERROR")
                    .setMessage(e.getMessage())
                    .build())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * Log received payload to file for verification
     */
    private void logReceivedPayload(String transactionId, String dbName, List<BatchOperation> operations) {
        try {
            StringBuilder payloadLog = new StringBuilder();
            payloadLog.append("RECEIVED|");
            payloadLog.append(TIMESTAMP_FORMAT.format(LocalDateTime.now())).append("|");
            payloadLog.append(transactionId).append("|");
            payloadLog.append(dbName).append("|");
            payloadLog.append(operations.size()).append("|");
            
            // Log each operation
            for (int i = 0; i < operations.size(); i++) {
                BatchOperation op = operations.get(i);
                payloadLog.append("OP").append(i + 1).append(":");
                payloadLog.append(op.getOperationType()).append("|");
                payloadLog.append(op.getEntityType()).append("|");
                
                // Log operation-specific details
                switch (op.getPayloadCase()) {
                    case PACKAGE_ACCOUNT:
                        PackageAccount account = op.getPackageAccount();
                        payloadLog.append("id=").append(account.getId())
                                  .append("|name=").append(account.getName())
                                  .append("|balanceAfter=").append(account.getBalanceAfter());
                        break;
                        
                    case PACKAGE_ACCOUNT_RESERVE:
                        PackageAccountReserve reserve = op.getPackageAccountReserve();
                        payloadLog.append("id=").append(reserve.getId())
                                  .append("|packageAccountId=").append(reserve.getPackageAccountId())
                                  .append("|reservedAmount=").append(reserve.getReservedAmount());
                        break;
                        
                    case PACKAGE_ACCOUNT_DELTA:
                        PackageAccountDelta accountDelta = op.getPackageAccountDelta();
                        payloadLog.append("accountId=").append(accountDelta.getAccountId())
                                  .append("|amount=").append(accountDelta.getAmount());
                        break;
                        
                    case PACKAGE_ACCOUNT_RESERVE_DELTA:
                        PackageAccountReserveDelta reserveDelta = op.getPackageAccountReserveDelta();
                        payloadLog.append("reserveId=").append(reserveDelta.getReserveId())
                                  .append("|amount=").append(reserveDelta.getAmount());
                        break;
                        
                    case PACKAGE_ACCOUNT_DELETE_DELTA:
                        PackageAccountDeleteDelta accountDelete = op.getPackageAccountDeleteDelta();
                        payloadLog.append("accountId=").append(accountDelete.getAccountId());
                        break;
                        
                    case PACKAGE_ACCOUNT_RESERVE_DELETE_DELTA:
                        PackageAccountReserveDeleteDelta reserveDelete = op.getPackageAccountReserveDeleteDelta();
                        payloadLog.append("reserveId=").append(reserveDelete.getReserveId());
                        break;
                        
                    default:
                        payloadLog.append("unknown_payload");
                }
                
                if (i < operations.size() - 1) {
                    payloadLog.append("|");
                }
            }
            
            // Write to file
            try (FileWriter writer = new FileWriter(SERVER_LOG_FILE, true)) {
                writer.write(payloadLog.toString() + "\n");
                writer.flush();
                logger.debug("📝 Payload logged to file: {}", payloadLog.toString());
            }
            
        } catch (IOException e) {
            logger.error("❌ Failed to log payload to file: ", e);
        }
    }
    
    /**
     * Process individual operation (simplified version)
     */
    private OperationResult processOperation(BatchOperation operation, String dbName, 
                                            String transactionId, int index) {
        EntityType entityType = operation.getEntityType();
        OperationType opType = operation.getOperationType();
        
        try {
            // For this test, we'll just simulate processing without actual cache operations
            // since the cache might not be initialized
            Thread.sleep(10); // Simulate processing time
            
            return OperationResult.newBuilder()
                .setOperationIndex(index)
                .setSuccess(true)
                .setMessage("Operation logged and simulated successfully")
                .setEntityType(entityType)
                .setOperationType(opType)
                .build();
                
        } catch (Exception e) {
            return OperationResult.newBuilder()
                .setOperationIndex(index)
                .setSuccess(false)
                .setMessage("Simulation failed: " + e.getMessage())
                .setEntityType(entityType)
                .setOperationType(opType)
                .build();
        }
    }
}