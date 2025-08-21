package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.grpc.batch.*;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import com.zaxxer.hikari.HikariDataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * gRPC service for batch CRUD operations with atomic transaction support
 */
// @GrpcService  // Temporarily disabled to test TestBatchGrpcService
public class BatchCrudGrpcService extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(BatchCrudGrpcService.class);
    
    @Inject
    HikariDataSource dataSource;
    
    @Override
    public void executeBatch(BatchRequest request, StreamObserver<BatchResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        String dbName = request.getDbName();
        List<BatchOperation> operations = request.getOperationsList();
        
        logger.info("========================================");
        logger.info("📥 Received batch request");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("Operations count: {}", operations.size());
        logger.info("========================================");
        
        Connection conn = null;
        boolean originalAutoCommit = true;
        List<OperationResult> results = new ArrayList<>();
        
        try {
            // Get connection and start transaction
            conn = dataSource.getConnection();
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            
            logger.info("🔄 Started database transaction for batch");
            
            // Process each operation
            for (int i = 0; i < operations.size(); i++) {
                BatchOperation operation = operations.get(i);
                OperationResult result = processOperation(conn, dbName, operation, i);
                results.add(result);
                
                if (!result.getSuccess()) {
                    throw new RuntimeException("Operation " + i + " failed: " + result.getMessage());
                }
            }
            
            // Commit transaction
            conn.commit();
            logger.info("✅ Successfully committed batch transaction with {} operations", operations.size());
            
            // Build success response
            BatchResponse response = BatchResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Batch processed successfully")
                .setTransactionId(transactionId)
                .setOperationsProcessed(operations.size())
                .addAllResults(results)
                .build();
                
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("❌ Error processing batch: ", e);
            
            // Rollback transaction
            if (conn != null) {
                try {
                    conn.rollback();
                    logger.info("🔙 Rolled back transaction due to error");
                } catch (SQLException rollbackEx) {
                    logger.error("Failed to rollback transaction", rollbackEx);
                }
            }
            
            // Build error response
            BatchResponse errorResponse = BatchResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Batch processing failed: " + e.getMessage())
                .setTransactionId(transactionId)
                .setOperationsProcessed(results.size())
                .addAllResults(results)
                .setError(ErrorDetails.newBuilder()
                    .setCode("BATCH_PROCESSING_ERROR")
                    .setMessage(e.getMessage())
                    .setFailedOperationIndex(results.size())
                    .build())
                .build();
                
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
            
        } finally {
            // Restore auto-commit and close connection
            if (conn != null) {
                try {
                    conn.setAutoCommit(originalAutoCommit);
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Error closing connection", e);
                }
            }
        }
    }
    
    private OperationResult processOperation(Connection conn, String dbName, 
                                            BatchOperation operation, int index) {
        EntityType entityType = operation.getEntityType();
        OperationType opType = operation.getOperationType();
        
        logger.info("Processing operation {}: {} on {}", 
            index, opType, entityType);
        
        try {
            switch (operation.getPayloadCase()) {
                case PACKAGE_ACCOUNT:
                    return processPackageAccount(conn, dbName, operation, index);
                    
                case PACKAGE_ACCOUNT_RESERVE:
                    return processPackageAccountReserve(conn, dbName, operation, index);
                    
                case PACKAGE_ACCOUNT_DELTA:
                    return processPackageAccountDelta(conn, dbName, operation, index);
                    
                case PACKAGE_ACCOUNT_RESERVE_DELTA:
                    return processPackageAccountReserveDelta(conn, dbName, operation, index);
                    
                case PACKAGE_ACCOUNT_DELETE_DELTA:
                    return processPackageAccountDeleteDelta(conn, dbName, operation, index);
                    
                case PACKAGE_ACCOUNT_RESERVE_DELETE_DELTA:
                    return processPackageAccountReserveDeleteDelta(conn, dbName, operation, index);
                    
                default:
                    throw new UnsupportedOperationException(
                        "Unsupported payload type: " + operation.getPayloadCase());
            }
        } catch (Exception e) {
            logger.error("Failed to process operation {}: ", index, e);
            return OperationResult.newBuilder()
                .setOperationIndex(index)
                .setSuccess(false)
                .setMessage(e.getMessage())
                .setEntityType(entityType)
                .setOperationType(opType)
                .build();
        }
    }
    
    private OperationResult processPackageAccount(Connection conn, String dbName, 
                                                 BatchOperation operation, int index) {
        PackageAccount payload = operation.getPackageAccount();
        
        logger.debug("  PackageAccount - ID: {}, Name: {}, Balance: {}", 
            payload.getId(), payload.getName(), payload.getBalanceAfter());
        
        // Log the received data
        logger.info("  📦 PackageAccount Details:");
        logger.info("    - ID: {}", payload.getId());
        logger.info("    - Package Purchase ID: {}", payload.getPackagePurchaseId());
        logger.info("    - Name: {}", payload.getName());
        logger.info("    - Last Amount: {}", payload.getLastAmount());
        logger.info("    - Balance Before: {}", payload.getBalanceBefore());
        logger.info("    - Balance After: {}", payload.getBalanceAfter());
        logger.info("    - UOM: {}", payload.getUom());
        logger.info("    - Is Selected: {}", payload.getIsSelected());
        
        // TODO: Actual database operation will be implemented later
        // For now, just return success
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage("PackageAccount operation simulated successfully")
            .setEntityType(operation.getEntityType())
            .setOperationType(operation.getOperationType())
            .build();
    }
    
    private OperationResult processPackageAccountReserve(Connection conn, String dbName, 
                                                        BatchOperation operation, int index) {
        PackageAccountReserve payload = operation.getPackageAccountReserve();
        
        logger.debug("  PackageAccountReserve - ID: {}, Session: {}, Amount: {}", 
            payload.getId(), payload.getSessionId(), payload.getReservedAmount());
        
        // Log the received data
        logger.info("  🔒 PackageAccountReserve Details:");
        logger.info("    - ID: {}", payload.getId());
        logger.info("    - Package Account ID: {}", payload.getPackageAccountId());
        logger.info("    - Session ID: {}", payload.getSessionId());
        logger.info("    - Reserved Amount: {}", payload.getReservedAmount());
        logger.info("    - Reserved At: {}", payload.getReservedAt());
        logger.info("    - Released At: {}", payload.getReleasedAt());
        logger.info("    - Status: {}", payload.getStatus());
        logger.info("    - Current Reserve: {}", payload.getCurrentReserve());
        
        // TODO: Actual database operation will be implemented later
        // For now, just return success
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage("PackageAccountReserve operation simulated successfully")
            .setEntityType(operation.getEntityType())
            .setOperationType(operation.getOperationType())
            .build();
    }
    
    private OperationResult processPackageAccountDelta(Connection conn, String dbName, 
                                                      BatchOperation operation, int index) {
        PackageAccountDelta payload = operation.getPackageAccountDelta();
        
        logger.debug("  PackageAccountDelta - AccountID: {}, Amount: {}", 
            payload.getAccountId(), payload.getAmount());
        
        // Log the received data
        logger.info("  📝 PackageAccount Delta Details:");
        logger.info("    - DB Name: {}", payload.getDbName());
        logger.info("    - Account ID: {}", payload.getAccountId());
        logger.info("    - Amount: {}", payload.getAmount());
        
        // TODO: Actual database operation will be implemented later
        // For now, just return success
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage("PackageAccountDelta operation simulated successfully")
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setOperationType(operation.getOperationType())
            .build();
    }
    
    private OperationResult processPackageAccountReserveDelta(Connection conn, String dbName, 
                                                             BatchOperation operation, int index) {
        PackageAccountReserveDelta payload = operation.getPackageAccountReserveDelta();
        
        logger.debug("  PackageAccountReserveDelta - ReserveID: {}, Amount: {}, Session: {}", 
            payload.getReserveId(), payload.getAmount(), payload.getSessionId());
        
        // Log the received data
        logger.info("  📝 PackageAccountReserve Delta Details:");
        logger.info("    - DB Name: {}", payload.getDbName());
        logger.info("    - Reserve ID: {}", payload.getReserveId());
        logger.info("    - Amount: {}", payload.getAmount());
        logger.info("    - Session ID: {}", payload.getSessionId());
        
        // TODO: Actual database operation will be implemented later
        // For now, just return success
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage("PackageAccountReserveDelta operation simulated successfully")
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setOperationType(operation.getOperationType())
            .build();
    }
    
    private OperationResult processPackageAccountDeleteDelta(Connection conn, String dbName, 
                                                            BatchOperation operation, int index) {
        PackageAccountDeleteDelta payload = operation.getPackageAccountDeleteDelta();
        
        logger.debug("  PackageAccountDeleteDelta - AccountID: {}", 
            payload.getAccountId());
        
        // Log the received data
        logger.info("  🗑️ PackageAccount Delete Delta Details:");
        logger.info("    - DB Name: {}", payload.getDbName());
        logger.info("    - Account ID: {}", payload.getAccountId());
        
        // TODO: Actual database operation will be implemented later
        // For now, just return success
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage("PackageAccountDeleteDelta operation simulated successfully")
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setOperationType(operation.getOperationType())
            .build();
    }
    
    private OperationResult processPackageAccountReserveDeleteDelta(Connection conn, String dbName, 
                                                                   BatchOperation operation, int index) {
        PackageAccountReserveDeleteDelta payload = operation.getPackageAccountReserveDeleteDelta();
        
        logger.debug("  PackageAccountReserveDeleteDelta - ReserveID: {}", 
            payload.getReserveId());
        
        // Log the received data
        logger.info("  🗑️ PackageAccountReserve Delete Delta Details:");
        logger.info("    - DB Name: {}", payload.getDbName());
        logger.info("    - Reserve ID: {}", payload.getReserveId());
        
        // TODO: Actual database operation will be implemented later
        // For now, just return success
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage("PackageAccountReserveDeleteDelta operation simulated successfully")
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setOperationType(operation.getOperationType())
            .build();
    }
}