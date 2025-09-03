package com.telcobright.examples.oltp.grpcController;

import com.telcobright.examples.oltp.grpc.batch.*;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test gRPC service that only logs received payloads without any database operations
 */
// @GrpcService  // Temporarily disabled to test GrpcServiceGenericCrud
public class TestBatchGrpcService extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(TestBatchGrpcService.class);
    
    @Override
    public void executeBatch(BatchRequest request, StreamObserver<BatchResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        String dbName = request.getDbName();
        List<BatchOperation> operations = request.getOperationsList();
        
        logger.info("\n╔══════════════════════════════════════════════════════╗");
        logger.info("║         📥 BATCH REQUEST RECEIVED                     ║");
        logger.info("╚══════════════════════════════════════════════════════╝");
        logger.info("📌 Transaction ID: {}", transactionId);
        logger.info("🗄️  Database: {}", dbName);
        logger.info("📊 Total Operations: {}", operations.size());
        logger.info("────────────────────────────────────────────────────────");
        
        List<OperationResult> results = new ArrayList<>();
        
        // Process and log each operation
        for (int i = 0; i < operations.size(); i++) {
            BatchOperation operation = operations.get(i);
            logger.info("\n▶️  OPERATION #{}", i + 1);
            logger.info("   Type: {}", operation.getOperationType());
            logger.info("   Entity: {}", operation.getEntityType());
            
            // Log detailed payload based on type
            switch (operation.getPayloadCase()) {
                case PACKAGE_ACCOUNT:
                    logPackageAccount(operation.getPackageAccount());
                    break;
                    
                case PACKAGE_ACCOUNT_RESERVE:
                    logPackageAccountReserve(operation.getPackageAccountReserve());
                    break;
                    
                case PACKAGE_ACCOUNT_DELTA:
                    logPackageAccountDelta(operation.getPackageAccountDelta());
                    break;
                    
                case PACKAGE_ACCOUNT_RESERVE_DELTA:
                    logPackageAccountReserveDelta(operation.getPackageAccountReserveDelta());
                    break;
                    
                case PACKAGE_ACCOUNT_DELETE_DELTA:
                    logPackageAccountDeleteDelta(operation.getPackageAccountDeleteDelta());
                    break;
                    
                case PACKAGE_ACCOUNT_RESERVE_DELETE_DELTA:
                    logPackageAccountReserveDeleteDelta(operation.getPackageAccountReserveDeleteDelta());
                    break;
                    
                default:
                    logger.warn("   ⚠️  Unknown payload type: {}", operation.getPayloadCase());
            }
            
            // Create success result for each operation
            results.add(OperationResult.newBuilder()
                .setOperationIndex(i)
                .setSuccess(true)
                .setMessage("Operation logged successfully")
                .setEntityType(operation.getEntityType())
                .setOperationType(operation.getOperationType())
                .build());
        }
        
        logger.info("\n════════════════════════════════════════════════════════");
        logger.info("✅ Batch processing complete - All operations logged");
        logger.info("════════════════════════════════════════════════════════\n");
        
        // Build and send response
        BatchResponse response = BatchResponse.newBuilder()
            .setSuccess(true)
            .setMessage("All operations received and logged successfully")
            .setTransactionId(transactionId)
            .setOperationsProcessed(operations.size())
            .addAllResults(results)
            .build();
            
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    
    private void logPackageAccount(PackageAccount payload) {
        logger.info("   📦 PACKAGE_ACCOUNT Details:");
        logger.info("      - ID: {}", payload.getId());
        logger.info("      - Package Purchase ID: {}", payload.getPackagePurchaseId());
        logger.info("      - Name: {}", payload.getName());
        logger.info("      - Last Amount: {}", payload.getLastAmount());
        logger.info("      - Balance Before: {}", payload.getBalanceBefore());
        logger.info("      - Balance After: {}", payload.getBalanceAfter());
        logger.info("      - UOM: {}", payload.getUom());
        logger.info("      - Is Selected: {}", payload.getIsSelected());
    }
    
    private void logPackageAccountReserve(PackageAccountReserve payload) {
        logger.info("   🔒 PACKAGE_ACCOUNT_RESERVE Details:");
        logger.info("      - ID: {}", payload.getId());
        logger.info("      - Package Account ID: {}", payload.getPackageAccountId());
        logger.info("      - Session ID: {}", payload.getSessionId());
        logger.info("      - Reserved Amount: {}", payload.getReservedAmount());
        logger.info("      - Reserved At: {}", payload.getReservedAt());
        logger.info("      - Released At: {}", payload.getReleasedAt());
        logger.info("      - Status: {}", payload.getStatus());
        logger.info("      - Current Reserve: {}", payload.getCurrentReserve());
    }
    
    private void logPackageAccountDelta(PackageAccountDelta payload) {
        logger.info("   📝 PACKAGE_ACCOUNT_DELTA Details:");
        logger.info("      - DB Name: {}", payload.getDbName());
        logger.info("      - Account ID: {}", payload.getAccountId());
        logger.info("      - Amount: {}", payload.getAmount());
    }
    
    private void logPackageAccountReserveDelta(PackageAccountReserveDelta payload) {
        logger.info("   📝 PACKAGE_ACCOUNT_RESERVE_DELTA Details:");
        logger.info("      - DB Name: {}", payload.getDbName());
        logger.info("      - Reserve ID: {}", payload.getReserveId());
        logger.info("      - Amount: {}", payload.getAmount());
        logger.info("      - Session ID: {}", payload.getSessionId());
    }
    
    private void logPackageAccountDeleteDelta(PackageAccountDeleteDelta payload) {
        logger.info("   🗑️  PACKAGE_ACCOUNT_DELETE_DELTA Details:");
        logger.info("      - DB Name: {}", payload.getDbName());
        logger.info("      - Account ID: {}", payload.getAccountId());
    }
    
    private void logPackageAccountReserveDeleteDelta(PackageAccountReserveDeleteDelta payload) {
        logger.info("   🗑️  PACKAGE_ACCOUNT_RESERVE_DELETE_DELTA Details:");
        logger.info("      - DB Name: {}", payload.getDbName());
        logger.info("      - Reserve ID: {}", payload.getReserveId());
    }
}