package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.grpc.batch.*;
import com.telcobright.oltp.dbCache.CacheManager;
import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccountReserveDelta;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generic gRPC service that extracts deltas/entities and applies them to the database cache
 */
@GrpcService
public class GrpcServiceGenericCrud extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServiceGenericCrud.class);
    
    @Inject
    CacheManager cacheManager;
    
    @Override
    public void executeBatch(BatchRequest request, StreamObserver<BatchResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        String dbName = request.getDbName();
        List<BatchOperation> operations = request.getOperationsList();
        
        logger.info("════════════════════════════════════════════════════════");
        logger.info("📥 Generic CRUD Batch Request Received");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("Operations count: {}", operations.size());
        logger.info("════════════════════════════════════════════════════════");
        
        List<OperationResult> results = new ArrayList<>();
        boolean allSuccess = true;
        
        // Process each operation
        for (int i = 0; i < operations.size(); i++) {
            BatchOperation operation = operations.get(i);
            logger.info("\n🔄 Processing Operation #{}", i + 1);
            
            try {
                OperationResult result = processOperation(operation, dbName, transactionId, i);
                results.add(result);
                
                if (!result.getSuccess()) {
                    allSuccess = false;
                    logger.error("❌ Operation {} failed: {}", i, result.getMessage());
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
        
        logger.info("\n════════════════════════════════════════════════════════");
        logger.info("{} Batch processing complete", allSuccess ? "✅" : "⚠️");
        logger.info("Success: {}, Processed: {}/{}", allSuccess, results.size(), operations.size());
        logger.info("════════════════════════════════════════════════════════\n");
        
        // Build response
        BatchResponse.Builder responseBuilder = BatchResponse.newBuilder()
            .setSuccess(allSuccess)
            .setTransactionId(transactionId)
            .setOperationsProcessed(results.size())
            .addAllResults(results);
        
        if (allSuccess) {
            responseBuilder.setMessage("All operations processed successfully");
        } else {
            responseBuilder.setMessage("Some operations failed")
                .setError(ErrorDetails.newBuilder()
                    .setCode("PARTIAL_FAILURE")
                    .setMessage("Check individual operation results")
                    .build());
        }
        
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
    
    private OperationResult processOperation(BatchOperation operation, String dbName, 
                                            String transactionId, int index) {
        EntityType entityType = operation.getEntityType();
        OperationType opType = operation.getOperationType();
        
        logger.info("  Entity: {}, Operation: {}", entityType, opType);
        
        // Process based on operation type
        switch (opType) {
            case INSERT:
                return processInsert(operation, dbName, transactionId, index);
                
            case UPDATE_DELTA:
                return processUpdateDelta(operation, dbName, transactionId, index);
                
            case DELETE_DELTA:
                return processDeleteDelta(operation, dbName, transactionId, index);
                
            default:
                return OperationResult.newBuilder()
                    .setOperationIndex(index)
                    .setSuccess(false)
                    .setMessage("Unsupported operation type: " + opType)
                    .setEntityType(entityType)
                    .setOperationType(opType)
                    .build();
        }
    }
    
    /**
     * Process INSERT operations
     */
    private OperationResult processInsert(BatchOperation operation, String dbName, 
                                         String transactionId, int index) {
        EntityType entityType = operation.getEntityType();
        
        switch (operation.getPayloadCase()) {
            case PACKAGE_ACCOUNT:
                PackageAccount account = operation.getPackageAccount();
                logger.info("  📦 INSERT PackageAccount: ID={}, Name={}", 
                    account.getId(), account.getName());
                
                // Dummy method call for inserting into cache
                applyPackageAccountInsert(dbName, account, transactionId);
                
                return createSuccessResult(index, entityType, OperationType.INSERT,
                    "PackageAccount inserted successfully");
                
            case PACKAGE_ACCOUNT_RESERVE:
                PackageAccountReserve reserve = operation.getPackageAccountReserve();
                logger.info("  🔒 INSERT PackageAccountReserve: ID={}, AccountID={}, Amount={}", 
                    reserve.getId(), reserve.getPackageAccountId(), reserve.getReservedAmount());
                
                // Dummy method call for inserting into cache
                applyPackageAccountReserveInsert(dbName, reserve, transactionId);
                
                return createSuccessResult(index, entityType, OperationType.INSERT,
                    "PackageAccountReserve inserted successfully");
                
            default:
                return createErrorResult(index, entityType, OperationType.INSERT,
                    "Unknown payload type for INSERT: " + operation.getPayloadCase());
        }
    }
    
    /**
     * Process UPDATE_DELTA operations
     */
    private OperationResult processUpdateDelta(BatchOperation operation, String dbName, 
                                              String transactionId, int index) {
        EntityType entityType = operation.getEntityType();
        
        switch (operation.getPayloadCase()) {
            case PACKAGE_ACCOUNT_DELTA:
                PackageAccountDelta accountDelta = operation.getPackageAccountDelta();
                logger.info("  📝 UPDATE PackageAccount Delta: AccountID={}, Amount={}", 
                    accountDelta.getAccountId(), accountDelta.getAmount());
                
                // Dummy method call for applying delta to cache
                applyPackageAccountDelta(dbName, accountDelta, transactionId);
                
                return createSuccessResult(index, entityType, OperationType.UPDATE_DELTA,
                    "PackageAccount delta applied successfully");
                
            case PACKAGE_ACCOUNT_RESERVE_DELTA:
                com.telcobright.oltp.grpc.batch.PackageAccountReserveDelta reserveDelta = operation.getPackageAccountReserveDelta();
                logger.info("  📝 UPDATE PackageAccountReserve Delta: ReserveID={}, Amount={}", 
                    reserveDelta.getReserveId(), reserveDelta.getAmount());
                
                // Dummy method call for applying delta to cache
                applyPackageAccountReserveDelta(dbName, reserveDelta, transactionId);
                
                return createSuccessResult(index, entityType, OperationType.UPDATE_DELTA,
                    "PackageAccountReserve delta applied successfully");
                
            default:
                return createErrorResult(index, entityType, OperationType.UPDATE_DELTA,
                    "Unknown payload type for UPDATE_DELTA: " + operation.getPayloadCase());
        }
    }
    
    /**
     * Process DELETE_DELTA operations
     */
    private OperationResult processDeleteDelta(BatchOperation operation, String dbName, 
                                              String transactionId, int index) {
        EntityType entityType = operation.getEntityType();
        
        switch (operation.getPayloadCase()) {
            case PACKAGE_ACCOUNT_DELETE_DELTA:
                PackageAccountDeleteDelta accountDelete = operation.getPackageAccountDeleteDelta();
                logger.info("  🗑️ DELETE PackageAccount: AccountID={}", 
                    accountDelete.getAccountId());
                
                // Dummy method call for deleting from cache
                applyPackageAccountDelete(dbName, accountDelete, transactionId);
                
                return createSuccessResult(index, entityType, OperationType.DELETE_DELTA,
                    "PackageAccount deleted successfully");
                
            case PACKAGE_ACCOUNT_RESERVE_DELETE_DELTA:
                PackageAccountReserveDeleteDelta reserveDelete = operation.getPackageAccountReserveDeleteDelta();
                logger.info("  🗑️ DELETE PackageAccountReserve: ReserveID={}", 
                    reserveDelete.getReserveId());
                
                // Dummy method call for deleting from cache
                applyPackageAccountReserveDelete(dbName, reserveDelete, transactionId);
                
                return createSuccessResult(index, entityType, OperationType.DELETE_DELTA,
                    "PackageAccountReserve deleted successfully");
                
            default:
                return createErrorResult(index, entityType, OperationType.DELETE_DELTA,
                    "Unknown payload type for DELETE_DELTA: " + operation.getPayloadCase());
        }
    }
    
    // ==================== DUMMY METHODS FOR CACHE OPERATIONS ====================
    
    /**
     * Apply PackageAccount insert to cache
     */
    private void applyPackageAccountInsert(String dbName, PackageAccount account, String transactionId) {
        logger.debug("    → Applying PackageAccount INSERT to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Account Data: ID={}, Name={}, Balance={}", 
            account.getId(), account.getName(), account.getBalanceAfter());
        
        // Convert gRPC PackageAccount to entity PackageAccount
        com.telcobright.oltp.entity.PackageAccount entityAccount = new com.telcobright.oltp.entity.PackageAccount();
        entityAccount.setId(account.getId());
        entityAccount.setPackagePurchaseId(account.getPackagePurchaseId());
        entityAccount.setName(account.getName());
        entityAccount.setLastAmount(new BigDecimal(account.getLastAmount()));
        entityAccount.setBalanceBefore(new BigDecimal(account.getBalanceBefore()));
        entityAccount.setBalanceAfter(new BigDecimal(account.getBalanceAfter()));
        entityAccount.setUom(account.getUom());
        entityAccount.setIsSelected(account.getIsSelected());
        
        // Apply to cache
        Map<String, List<com.telcobright.oltp.entity.PackageAccount>> entities = new HashMap<>();
        entities.put(dbName, List.of(entityAccount));
        cacheManager.getPackageAccountCache().applyInserts(entities, transactionId);
        
        logger.info("    ✅ PackageAccount INSERT applied to cache");
    }
    
    /**
     * Apply PackageAccountReserve insert to cache
     */
    private void applyPackageAccountReserveInsert(String dbName, PackageAccountReserve reserve, String transactionId) {
        logger.debug("    → Applying PackageAccountReserve INSERT to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Reserve Data: ID={}, AccountID={}, Amount={}", 
            reserve.getId(), reserve.getPackageAccountId(), reserve.getReservedAmount());
        
        // Convert gRPC PackageAccountReserve to entity PackageAccountReserve
        com.telcobright.oltp.entity.PackageAccountReserve entityReserve = new com.telcobright.oltp.entity.PackageAccountReserve();
        entityReserve.setId(reserve.getId());
        entityReserve.setPackageAccountId(reserve.getPackageAccountId());
        entityReserve.setSessionId(reserve.getSessionId());
        entityReserve.setReservedAmount(new BigDecimal(reserve.getReservedAmount()));
        entityReserve.setCurrentReserve(new BigDecimal(reserve.getCurrentReserve()));
        entityReserve.setStatus(reserve.getStatus());
        
        // Parse dates if provided
        if (reserve.getReservedAt() != null && !reserve.getReservedAt().isEmpty()) {
            entityReserve.setReservedAt(LocalDateTime.parse(reserve.getReservedAt(), DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }
        if (reserve.getReleasedAt() != null && !reserve.getReleasedAt().isEmpty()) {
            entityReserve.setReleasedAt(LocalDateTime.parse(reserve.getReleasedAt(), DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }
        
        // Apply to cache
        Map<String, List<com.telcobright.oltp.entity.PackageAccountReserve>> entities = new HashMap<>();
        entities.put(dbName, List.of(entityReserve));
        cacheManager.getPackageAccountReserveCache().applyInserts(entities, transactionId);
        
        logger.info("    ✅ PackageAccountReserve INSERT applied to cache");
    }
    
    /**
     * Apply PackageAccount delta to cache
     */
    private void applyPackageAccountDelta(String dbName, PackageAccountDelta delta, String transactionId) {
        logger.debug("    → Applying PackageAccount DELTA to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Delta: AccountID={}, Amount={}", delta.getAccountId(), delta.getAmount());
        
        // Prepare balance update
        Map<String, Map<Long, BigDecimal>> updates = new HashMap<>();
        Map<Long, BigDecimal> accountDeltas = new HashMap<>();
        accountDeltas.put((long) delta.getAccountId(), new BigDecimal(delta.getAmount()));
        updates.put(dbName, accountDeltas);
        
        // Use the cache's prepareBalanceUpdates to get updated entities
        Map<String, List<com.telcobright.oltp.entity.PackageAccount>> preparedUpdates = 
            cacheManager.getPackageAccountCache().prepareBalanceUpdates(updates);
        
        // Apply the updates to cache
        if (!preparedUpdates.isEmpty()) {
            cacheManager.getPackageAccountCache().applyUpdates(preparedUpdates, transactionId);
            logger.info("    ✅ PackageAccount DELTA applied to cache");
        } else {
            logger.warn("    ⚠️ No accounts found to apply delta");
        }
    }
    
    /**
     * Apply PackageAccountReserve delta to cache
     */
    private void applyPackageAccountReserveDelta(String dbName, com.telcobright.oltp.grpc.batch.PackageAccountReserveDelta delta, String transactionId) {
        logger.debug("    → Applying PackageAccountReserve DELTA to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Delta: ReserveID={}, Amount={}, SessionID={}", 
            delta.getReserveId(), delta.getAmount(), delta.getSessionId());
        
        // Get the existing reserve
        com.telcobright.oltp.entity.PackageAccountReserve reserve = 
            cacheManager.getPackageAccountReserveCache().get(dbName, (long) delta.getReserveId());
        
        if (reserve != null) {
            // Apply the delta
            BigDecimal deltaAmount = new BigDecimal(delta.getAmount());
            reserve.applyDelta(deltaAmount);
            
            // Update session ID if provided
            if (delta.getSessionId() != null && !delta.getSessionId().isEmpty()) {
                reserve.setSessionId(delta.getSessionId());
            }
            
            // Apply update to cache
            Map<String, List<com.telcobright.oltp.entity.PackageAccountReserve>> updates = new HashMap<>();
            updates.put(dbName, List.of(reserve));
            cacheManager.getPackageAccountReserveCache().applyUpdates(updates, transactionId);
            
            logger.info("    ✅ PackageAccountReserve DELTA applied to cache");
        } else {
            logger.warn("    ⚠️ Reserve not found for delta: ReserveID={}", delta.getReserveId());
        }
    }
    
    /**
     * Apply PackageAccount delete to cache
     */
    private void applyPackageAccountDelete(String dbName, PackageAccountDeleteDelta delete, String transactionId) {
        logger.debug("    → Applying PackageAccount DELETE to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Delete: AccountID={}", delete.getAccountId());
        
        // Apply delete to cache
        Map<String, List<Long>> ids = new HashMap<>();
        ids.put(dbName, List.of((long) delete.getAccountId()));
        cacheManager.getPackageAccountCache().applyDeletes(ids, transactionId);
        
        logger.info("    ✅ PackageAccount DELETE applied to cache");
    }
    
    /**
     * Apply PackageAccountReserve delete to cache
     */
    private void applyPackageAccountReserveDelete(String dbName, PackageAccountReserveDeleteDelta delete, String transactionId) {
        logger.debug("    → Applying PackageAccountReserve DELETE to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Delete: ReserveID={}", delete.getReserveId());
        
        // Apply delete to cache
        Map<String, List<Long>> ids = new HashMap<>();
        ids.put(dbName, List.of((long) delete.getReserveId()));
        cacheManager.getPackageAccountReserveCache().applyDeletes(ids, transactionId);
        
        logger.info("    ✅ PackageAccountReserve DELETE applied to cache");
    }
    
    // ==================== HELPER METHODS ====================
    
    private OperationResult createSuccessResult(int index, EntityType entityType, 
                                               OperationType opType, String message) {
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(true)
            .setMessage(message)
            .setEntityType(entityType)
            .setOperationType(opType)
            .build();
    }
    
    private OperationResult createErrorResult(int index, EntityType entityType, 
                                             OperationType opType, String message) {
        return OperationResult.newBuilder()
            .setOperationIndex(index)
            .setSuccess(false)
            .setMessage(message)
            .setEntityType(entityType)
            .setOperationType(opType)
            .build();
    }
}