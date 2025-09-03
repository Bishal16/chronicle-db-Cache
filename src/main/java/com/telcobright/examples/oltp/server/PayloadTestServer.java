package com.telcobright.examples.oltp.server;

import com.telcobright.examples.oltp.grpc.batch.*;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone gRPC server with embedded Quarkus runtime for payload testing.
 * Run this class directly to start the gRPC server on port 9000.
 * 
 * Usage: java -cp ... com.telcobright.oltp.server.PayloadTestServer
 */
// @QuarkusMain  // Disabled to avoid conflict with SimplePayloadTestServer
public class PayloadTestServer implements QuarkusApplication {
    private static final Logger logger = LoggerFactory.getLogger(PayloadTestServer.class);
    
    public static void main(String[] args) {
        System.setProperty("quarkus.grpc.server.port", "9000");
        System.setProperty("quarkus.http.port", "8085");
        
        logger.info("╔══════════════════════════════════════════════════════════════╗");
        logger.info("║                  Payload Test gRPC Server                     ║");
        logger.info("║                  Starting on port 9000...                     ║");
        logger.info("╚══════════════════════════════════════════════════════════════╝");
        
        Quarkus.run(PayloadTestServer.class, args);
    }
    
    @Override
    public int run(String... args) throws Exception {
        logger.info("🚀 Payload Test Server started successfully!");
        logger.info("🔌 gRPC Server listening on port 9000");
        logger.info("🌐 HTTP Server listening on port 8085");
        logger.info("📝 Server logs will be written to: server-received-payloads.log");
        logger.info("🛑 Press Ctrl+C to stop the server");
        
        Quarkus.waitForExit();
        return 0;
    }
    
    /**
     * Embedded gRPC service for handling batch operations
     */
    // @GrpcService  // Disabled to avoid conflict with GrpcServiceGenericCrud
    public static class EmbeddedPayloadLoggingService extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
        private static final Logger serviceLogger = LoggerFactory.getLogger(EmbeddedPayloadLoggingService.class);
        private static final String SERVER_LOG_FILE = "server-received-payloads.log";
        private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        
        @Override
        public void executeBatch(BatchRequest request, StreamObserver<BatchResponse> responseObserver) {
            String transactionId = request.getTransactionId();
            String dbName = request.getDbName();
            List<BatchOperation> operations = request.getOperationsList();
            
            serviceLogger.info("════════════════════════════════════════════════════════");
            serviceLogger.info("📥 Embedded gRPC Service - Batch Request Received");
            serviceLogger.info("Transaction ID: {}", transactionId);
            serviceLogger.info("Database: {}", dbName);
            serviceLogger.info("Operations count: {}", operations.size());
            serviceLogger.info("════════════════════════════════════════════════════════");
            
            try {
                // Log the received payload to file
                logReceivedPayload(transactionId, dbName, operations);
                
                // Process operations
                List<OperationResult> results = new ArrayList<>();
                boolean allSuccess = true;
                
                for (int i = 0; i < operations.size(); i++) {
                    BatchOperation operation = operations.get(i);
                    serviceLogger.info("🔄 Processing Operation #{}: {} {}", 
                        i + 1, operation.getOperationType(), operation.getEntityType());
                    
                    // Log operation details
                    logOperationDetails(operation, i + 1);
                    
                    // Create success result (simulated processing)
                    OperationResult result = OperationResult.newBuilder()
                        .setOperationIndex(i)
                        .setSuccess(true)
                        .setMessage("Operation received and logged successfully")
                        .setEntityType(operation.getEntityType())
                        .setOperationType(operation.getOperationType())
                        .build();
                    
                    results.add(result);
                    serviceLogger.info("✅ Operation {} completed successfully", i + 1);
                }
                
                serviceLogger.info("════════════════════════════════════════════════════════");
                serviceLogger.info("✅ All {} operations processed successfully", results.size());
                serviceLogger.info("📝 Payload logged to: {}", SERVER_LOG_FILE);
                serviceLogger.info("════════════════════════════════════════════════════════");
                
                // Build and send response
                BatchResponse response = BatchResponse.newBuilder()
                    .setSuccess(allSuccess)
                    .setTransactionId(transactionId)
                    .setOperationsProcessed(results.size())
                    .setMessage("All operations received, logged, and processed successfully by embedded server")
                    .addAllResults(results)
                    .build();
                
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                
            } catch (Exception e) {
                serviceLogger.error("❌ Error processing batch request: ", e);
                
                BatchResponse errorResponse = BatchResponse.newBuilder()
                    .setSuccess(false)
                    .setTransactionId(transactionId)
                    .setOperationsProcessed(0)
                    .setMessage("Server error: " + e.getMessage())
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
                }
                
                serviceLogger.debug("📝 Payload logged: {}", payloadLog.toString());
                
            } catch (IOException e) {
                serviceLogger.error("❌ Failed to log payload to file: ", e);
            }
        }
        
        /**
         * Log detailed operation information
         */
        private void logOperationDetails(BatchOperation operation, int opNumber) {
            switch (operation.getPayloadCase()) {
                case PACKAGE_ACCOUNT:
                    PackageAccount account = operation.getPackageAccount();
                    serviceLogger.info("   📦 PackageAccount: ID={}, Name={}, Balance={}", 
                        account.getId(), account.getName(), account.getBalanceAfter());
                    break;
                    
                case PACKAGE_ACCOUNT_RESERVE:
                    PackageAccountReserve reserve = operation.getPackageAccountReserve();
                    serviceLogger.info("   🔒 PackageAccountReserve: ID={}, AccountID={}, Amount={}", 
                        reserve.getId(), reserve.getPackageAccountId(), reserve.getReservedAmount());
                    break;
                    
                case PACKAGE_ACCOUNT_DELTA:
                    PackageAccountDelta accountDelta = operation.getPackageAccountDelta();
                    serviceLogger.info("   📝 PackageAccountDelta: AccountID={}, Amount={}", 
                        accountDelta.getAccountId(), accountDelta.getAmount());
                    break;
                    
                case PACKAGE_ACCOUNT_RESERVE_DELTA:
                    PackageAccountReserveDelta reserveDelta = operation.getPackageAccountReserveDelta();
                    serviceLogger.info("   📝 PackageAccountReserveDelta: ReserveID={}, Amount={}", 
                        reserveDelta.getReserveId(), reserveDelta.getAmount());
                    break;
                    
                case PACKAGE_ACCOUNT_DELETE_DELTA:
                    PackageAccountDeleteDelta accountDelete = operation.getPackageAccountDeleteDelta();
                    serviceLogger.info("   🗑️ PackageAccountDeleteDelta: AccountID={}", 
                        accountDelete.getAccountId());
                    break;
                    
                case PACKAGE_ACCOUNT_RESERVE_DELETE_DELTA:
                    PackageAccountReserveDeleteDelta reserveDelete = operation.getPackageAccountReserveDeleteDelta();
                    serviceLogger.info("   🗑️ PackageAccountReserveDeleteDelta: ReserveID={}", 
                        reserveDelete.getReserveId());
                    break;
                    
                default:
                    serviceLogger.warn("   ⚠️ Unknown payload type: {}", operation.getPayloadCase());
            }
        }
    }
}