package com.telcobright.examples.oltp.grpcController;

import com.telcobright.examples.oltp.grpc.batch.*;
import com.telcobright.examples.oltp.dbCache.CacheManager;
import com.telcobright.examples.oltp.entity.PackageAccDelta;
import com.telcobright.examples.oltp.entity.PackageAccountReserveDelta;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.core.wal.WALWriter;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import java.io.FileWriter;
import java.io.IOException;
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
// @GrpcService // Disabled in favor of WALBatchGrpcService
public class GrpcServiceGenericCrud extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServiceGenericCrud.class);
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
        logger.info("📥 Generic CRUD Batch Request Received");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("Operations count: {}", operations.size());
        logger.info("════════════════════════════════════════════════════════");
        
        // Convert operations to WALEntryBatch for WAL processing
        WALEntryBatch walBatch = convertOperationsToWALEntryBatch(operations, transactionId, dbName);
        
        // Log received payload to file for verification
        logReceivedPayload(transactionId, dbName, operations);
        
        // Log WALEntryBatch details for verification
        logWALEntryBatch(walBatch);
        
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
                com.telcobright.examples.oltp.grpc.batch.PackageAccountReserveDelta reserveDelta = operation.getPackageAccountReserveDelta();
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
        com.telcobright.examples.oltp.entity.PackageAccount entityAccount = new com.telcobright.examples.oltp.entity.PackageAccount();
        entityAccount.setId(account.getId());
        entityAccount.setPackagePurchaseId(account.getPackagePurchaseId());
        entityAccount.setName(account.getName());
        entityAccount.setLastAmount(new BigDecimal(account.getLastAmount()));
        entityAccount.setBalanceBefore(new BigDecimal(account.getBalanceBefore()));
        entityAccount.setBalanceAfter(new BigDecimal(account.getBalanceAfter()));
        entityAccount.setUom(account.getUom());
        entityAccount.setIsSelected(account.getIsSelected());
        
        // Apply to cache
        Map<String, List<com.telcobright.examples.oltp.entity.PackageAccount>> entities = new HashMap<>();
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
        com.telcobright.examples.oltp.entity.PackageAccountReserve entityReserve = new com.telcobright.examples.oltp.entity.PackageAccountReserve();
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
        Map<String, List<com.telcobright.examples.oltp.entity.PackageAccountReserve>> entities = new HashMap<>();
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
        Map<String, List<com.telcobright.examples.oltp.entity.PackageAccount>> preparedUpdates = 
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
    private void applyPackageAccountReserveDelta(String dbName, com.telcobright.examples.oltp.grpc.batch.PackageAccountReserveDelta delta, String transactionId) {
        logger.debug("    → Applying PackageAccountReserve DELTA to cache");
        logger.debug("      DB: {}, TxnID: {}", dbName, transactionId);
        logger.debug("      Delta: ReserveID={}, Amount={}, SessionID={}", 
            delta.getReserveId(), delta.getAmount(), delta.getSessionId());
        
        // Get the existing reserve
        com.telcobright.examples.oltp.entity.PackageAccountReserve reserve = 
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
            Map<String, List<com.telcobright.examples.oltp.entity.PackageAccountReserve>> updates = new HashMap<>();
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
    
    /**
     * Convert BatchOperations to WALEntryBatch for WAL processing
     */
    private WALEntryBatch convertOperationsToWALEntryBatch(List<BatchOperation> operations, String transactionId, String dbName) {
        List<WALEntry> walEntries = new ArrayList<>();
        
        for (BatchOperation operation : operations) {
            WALEntry entry = convertOperationToWALEntry(operation, dbName);
            if (entry != null) {
                walEntries.add(entry);
            }
        }
        
        logger.info("Converted {} operations to {} WALEntries in batch", operations.size(), walEntries.size());
        return WALEntryBatch.builder()
            .transactionId(transactionId)
            .entries(walEntries)
            .build();
    }
    
    /**
     * Convert a single BatchOperation to WALEntry
     */
    private WALEntry convertOperationToWALEntry(BatchOperation operation, String dbName) {
        WALEntry.Builder builder = WALEntry.builder()
            .dbName(dbName);
            // Transaction ID managed at WALEntryBatch level
        
        switch (operation.getEntityType()) {
            case PACKAGE_ACCOUNT:
                return convertPackageAccountOperationToWAL(operation, builder);
            case PACKAGE_ACCOUNT_RESERVE:
                return convertPackageAccountReserveOperationToWAL(operation, builder);
            default:
                logger.warn("Unsupported entity type for WAL conversion: {}", operation.getEntityType());
                return null;
        }
    }
    
    /**
     * Convert PackageAccount operations to WALEntry
     */
    private WALEntry convertPackageAccountOperationToWAL(BatchOperation operation, WALEntry.Builder builder) {
        builder.tableName("packageaccount");
        
        switch (operation.getOperationType()) {
            case UPDATE_DELTA:
                PackageAccountDelta delta = operation.getPackageAccountDelta();
                builder.operationType(WALEntry.OperationType.UPDATE)
                    .withData("accountId", (long) delta.getAccountId())
                    .withData("amount", new BigDecimal(delta.getAmount()));
                break;
            case INSERT:
                PackageAccount account = operation.getPackageAccount();
                builder.operationType(WALEntry.OperationType.INSERT)
                    .withData("id", account.getId())
                    .withData("packagePurchaseId", account.getPackagePurchaseId())
                    .withData("name", account.getName())
                    .withData("lastAmount", new BigDecimal(account.getLastAmount()))
                    .withData("balanceBefore", new BigDecimal(account.getBalanceBefore()))
                    .withData("balanceAfter", new BigDecimal(account.getBalanceAfter()))
                    .withData("uom", account.getUom())
                    .withData("isSelected", account.getIsSelected());
                break;
            case DELETE_DELTA:
                PackageAccountDeleteDelta deleteDelta = operation.getPackageAccountDeleteDelta();
                builder.operationType(WALEntry.OperationType.DELETE)
                    .withData("accountId", (long) deleteDelta.getAccountId());
                break;
            default:
                logger.warn("Unsupported operation type for PackageAccount WAL: {}", operation.getOperationType());
                return null;
        }
        
        return builder.build();
    }
    
    /**
     * Convert PackageAccountReserve operations to WALEntry
     */
    private WALEntry convertPackageAccountReserveOperationToWAL(BatchOperation operation, WALEntry.Builder builder) {
        builder.tableName("packageaccountreserve");
        
        switch (operation.getOperationType()) {
            case INSERT:
                PackageAccountReserve reserve = operation.getPackageAccountReserve();
                builder.operationType(WALEntry.OperationType.INSERT)
                    .withData("id", reserve.getId())
                    .withData("packageAccountId", reserve.getPackageAccountId())
                    .withData("sessionId", reserve.getSessionId())
                    .withData("reservedAmount", new BigDecimal(reserve.getReservedAmount()))
                    .withData("currentReserve", new BigDecimal(reserve.getCurrentReserve()))
                    .withData("status", reserve.getStatus());
                if (!reserve.getReservedAt().isEmpty()) {
                    builder.withData("reservedAt", reserve.getReservedAt());
                }
                if (!reserve.getReleasedAt().isEmpty()) {
                    builder.withData("releasedAt", reserve.getReleasedAt());
                }
                break;
            case UPDATE_DELTA:
                com.telcobright.examples.oltp.grpc.batch.PackageAccountReserveDelta reserveDelta = operation.getPackageAccountReserveDelta();
                builder.operationType(WALEntry.OperationType.UPDATE)
                    .withData("reserveId", (long) reserveDelta.getReserveId())
                    .withData("amount", new BigDecimal(reserveDelta.getAmount()))
                    .withData("sessionId", reserveDelta.getSessionId());
                break;
            case DELETE_DELTA:
                PackageAccountReserveDeleteDelta deleteReserveDelta = operation.getPackageAccountReserveDeleteDelta();
                builder.operationType(WALEntry.OperationType.DELETE)
                    .withData("reserveId", (long) deleteReserveDelta.getReserveId());
                break;
            default:
                logger.warn("Unsupported operation type for PackageAccountReserve WAL: {}", operation.getOperationType());
                return null;
        }
        
        return builder.build();
    }
    
    /**
     * Log WALEntryBatch details for verification
     */
    private void logWALEntryBatch(WALEntryBatch walBatch) {
        logger.info("WALEntryBatch Details: {}", walBatch.toString());
        for (int i = 0; i < walBatch.size(); i++) {
            WALEntry entry = walBatch.get(i);
            logger.info("  WALEntry[{}]: db={}, table={}, op={}", 
                i, entry.getDbName(), entry.getTableName(), entry.getOperationType());
            logger.info("  WALEntry[{}] Data: {}", i, entry.getData());
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
            
            // Log each operation using WAL-like format
            for (int i = 0; i < operations.size(); i++) {
                BatchOperation op = operations.get(i);
                payloadLog.append("OP").append(i + 1).append(":");
                
                // Map operation types to WAL types for consistency
                switch (op.getOperationType()) {
                    case UPDATE_DELTA:
                        payloadLog.append("UPDATE|");
                        break;
                    case INSERT:
                        payloadLog.append("INSERT|");
                        break;
                    case DELETE_DELTA:
                        payloadLog.append("DELETE|");
                        break;
                    default:
                        payloadLog.append(op.getOperationType().name()).append("|");
                }
                
                // Use WAL table names for consistency
                switch (op.getEntityType()) {
                    case PACKAGE_ACCOUNT:
                        payloadLog.append("packageaccount|");
                        break;
                    case PACKAGE_ACCOUNT_RESERVE:
                        payloadLog.append("packageaccountreserve|");
                        break;
                    default:
                        payloadLog.append(op.getEntityType().name()).append("|");
                }
                
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
                        com.telcobright.examples.oltp.grpc.batch.PackageAccountReserveDelta reserveDelta = op.getPackageAccountReserveDelta();
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
            
            logger.debug("📝 Payload logged to file: {}", payloadLog.toString());
            
        } catch (IOException e) {
            logger.error("❌ Failed to log payload to file: ", e);
        }
    }
}