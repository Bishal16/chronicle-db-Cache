package com.telcobright.oltp.grpc.builder;

import com.telcobright.oltp.grpc.batch.*;
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
 * Fluent builder for constructing and submitting batch CRUD operations to gRPC service.
 * 
 * Usage examples:
 * 
 * // Case 1: Update PackageAccount with delta and insert reserve
 * BatchResponse response = GrpcCrudPayloadBuilder.create("localhost", 9000)
 *     .withDatabase("res_1")
 *     .withTransactionId("TXN_001")
 *     .updatePackageAccount(1001, "75.50")
 *     .insertPackageAccountReserve(4001, 1001, "30.00", "SESSION_001")
 *     .submit();
 * 
 * // Case 2: Update PackageAccount and PackageAccountReserve with deltas
 * BatchResponse response = GrpcCrudPayloadBuilder.create("localhost", 9000)
 *     .withDatabase("res_1")
 *     .updatePackageAccount(2001, "-25.75")
 *     .updatePackageAccountReserve(4001, "-15.00", "SESSION_002")
 *     .submit();
 * 
 * // Case 3: Delete PackageAccountReserve
 * BatchResponse response = GrpcCrudPayloadBuilder.create("localhost", 9000)
 *     .withDatabase("res_1")
 *     .deletePackageAccountReserve(5555)
 *     .submit();
 */
public class GrpcCrudPayloadBuilder {
    private static final Logger logger = LoggerFactory.getLogger(GrpcCrudPayloadBuilder.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    private final String host;
    private final int port;
    private String dbName = "res_1"; // Default database
    private String transactionId;
    private final List<BatchOperation> operations = new ArrayList<>();
    
    // Private constructor - use factory method
    private GrpcCrudPayloadBuilder(String host, int port) {
        this.host = host;
        this.port = port;
        this.transactionId = "TXN_" + UUID.randomUUID().toString();
    }
    
    /**
     * Create a new builder instance
     * @param host gRPC server host
     * @param port gRPC server port
     * @return New builder instance
     */
    public static GrpcCrudPayloadBuilder create(String host, int port) {
        return new GrpcCrudPayloadBuilder(host, port);
    }
    
    /**
     * Create a builder with default localhost:9000
     * @return New builder instance
     */
    public static GrpcCrudPayloadBuilder create() {
        return new GrpcCrudPayloadBuilder("localhost", 9000);
    }
    
    /**
     * Set the database name for all operations
     * @param dbName Database name
     * @return This builder
     */
    public GrpcCrudPayloadBuilder withDatabase(String dbName) {
        this.dbName = dbName;
        return this;
    }
    
    /**
     * Set a custom transaction ID
     * @param transactionId Transaction ID
     * @return This builder
     */
    public GrpcCrudPayloadBuilder withTransactionId(String transactionId) {
        this.transactionId = transactionId;
        return this;
    }
    
    // ==================== INSERT OPERATIONS ====================
    
    /**
     * Add a PackageAccount INSERT operation
     * @param id Account ID
     * @param packagePurchaseId Package purchase ID
     * @param name Account name
     * @param balanceAfter Balance after
     * @param uom Unit of measure
     * @return This builder
     */
    public GrpcCrudPayloadBuilder insertPackageAccount(long id, long packagePurchaseId, 
                                                       String name, String balanceAfter, String uom) {
        PackageAccount account = PackageAccount.newBuilder()
            .setId(id)
            .setPackagePurchaseId(packagePurchaseId)
            .setName(name)
            .setLastAmount("0.00")
            .setBalanceBefore(balanceAfter)
            .setBalanceAfter(balanceAfter)
            .setUom(uom)
            .setIsSelected(true)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccount(account)
            .build());
        
        logger.debug("Added INSERT PackageAccount: ID={}, Name={}", id, name);
        return this;
    }
    
    /**
     * Add a PackageAccountReserve INSERT operation
     * @param id Reserve ID
     * @param packageAccountId Package account ID
     * @param reservedAmount Reserved amount
     * @param sessionId Session ID
     * @return This builder
     */
    public GrpcCrudPayloadBuilder insertPackageAccountReserve(long id, long packageAccountId, 
                                                             String reservedAmount, String sessionId) {
        return insertPackageAccountReserve(id, packageAccountId, reservedAmount, sessionId, "ACTIVE");
    }
    
    /**
     * Add a PackageAccountReserve INSERT operation with status
     * @param id Reserve ID
     * @param packageAccountId Package account ID
     * @param reservedAmount Reserved amount
     * @param sessionId Session ID
     * @param status Reserve status
     * @return This builder
     */
    public GrpcCrudPayloadBuilder insertPackageAccountReserve(long id, long packageAccountId, 
                                                             String reservedAmount, String sessionId, String status) {
        PackageAccountReserve reserve = PackageAccountReserve.newBuilder()
            .setId(id)
            .setPackageAccountId(packageAccountId)
            .setSessionId(sessionId)
            .setReservedAmount(reservedAmount)
            .setReservedAt(LocalDateTime.now().format(DATE_FORMATTER))
            .setStatus(status)
            .setCurrentReserve(reservedAmount)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserve(reserve)
            .build());
        
        logger.debug("Added INSERT PackageAccountReserve: ID={}, AccountID={}, Amount={}", 
            id, packageAccountId, reservedAmount);
        return this;
    }
    
    // ==================== UPDATE OPERATIONS ====================
    
    /**
     * Add a PackageAccount UPDATE_DELTA operation (Case 1 & 2: op2)
     * @param accountId Account ID to update
     * @param deltaAmount Amount to add/subtract
     * @return This builder
     */
    public GrpcCrudPayloadBuilder updatePackageAccount(long accountId, String deltaAmount) {
        PackageAccountDelta delta = PackageAccountDelta.newBuilder()
            .setDbName(dbName)
            .setAccountId((int) accountId)
            .setAmount(deltaAmount)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDelta(delta)
            .build());
        
        logger.debug("Added UPDATE PackageAccount Delta: AccountID={}, Amount={}", accountId, deltaAmount);
        return this;
    }
    
    /**
     * Add a PackageAccountReserve UPDATE_DELTA operation (Case 2: op4)
     * @param reserveId Reserve ID to update
     * @param deltaAmount Amount to add/subtract (negative for release)
     * @param sessionId Session ID
     * @return This builder
     */
    public GrpcCrudPayloadBuilder updatePackageAccountReserve(long reserveId, String deltaAmount, String sessionId) {
        PackageAccountReserveDelta delta = PackageAccountReserveDelta.newBuilder()
            .setDbName(dbName)
            .setReserveId((int) reserveId)
            .setAmount(deltaAmount)
            .setSessionId(sessionId)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDelta(delta)
            .build());
        
        logger.debug("Added UPDATE PackageAccountReserve Delta: ReserveID={}, Amount={}", reserveId, deltaAmount);
        return this;
    }
    
    // ==================== DELETE OPERATIONS ====================
    
    /**
     * Add a PackageAccount DELETE_DELTA operation
     * @param accountId Account ID to delete
     * @return This builder
     */
    public GrpcCrudPayloadBuilder deletePackageAccount(long accountId) {
        PackageAccountDeleteDelta delta = PackageAccountDeleteDelta.newBuilder()
            .setDbName(dbName)
            .setAccountId((int) accountId)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.DELETE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDeleteDelta(delta)
            .build());
        
        logger.debug("Added DELETE PackageAccount: AccountID={}", accountId);
        return this;
    }
    
    /**
     * Add a PackageAccountReserve DELETE_DELTA operation (Case 3: op5)
     * @param reserveId Reserve ID to delete
     * @return This builder
     */
    public GrpcCrudPayloadBuilder deletePackageAccountReserve(long reserveId) {
        PackageAccountReserveDeleteDelta delta = PackageAccountReserveDeleteDelta.newBuilder()
            .setDbName(dbName)
            .setReserveId((int) reserveId)
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.DELETE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDeleteDelta(delta)
            .build());
        
        logger.debug("Added DELETE PackageAccountReserve: ReserveID={}", reserveId);
        return this;
    }
    
    // ==================== CONVENIENCE METHODS FOR TEST CASES ====================
    
    /**
     * Configure for Test Case 1: Update PackageAccount + Insert Reserve
     * @param accountId Account ID
     * @param deltaAmount Delta amount for account
     * @param reserveId Reserve ID
     * @param reserveAmount Reserve amount
     * @param sessionId Session ID
     * @return This builder
     */
    public GrpcCrudPayloadBuilder testCase1(long accountId, String deltaAmount, 
                                           long reserveId, String reserveAmount, String sessionId) {
        return this
            .updatePackageAccount(accountId, deltaAmount)
            .insertPackageAccountReserve(reserveId, accountId, reserveAmount, sessionId);
    }
    
    /**
     * Configure for Test Case 2: Update PackageAccount + Update Reserve
     * @param accountId Account ID
     * @param accountDelta Delta amount for account
     * @param reserveId Reserve ID
     * @param reserveDelta Delta amount for reserve
     * @param sessionId Session ID
     * @return This builder
     */
    public GrpcCrudPayloadBuilder testCase2(long accountId, String accountDelta,
                                           long reserveId, String reserveDelta, String sessionId) {
        return this
            .updatePackageAccount(accountId, accountDelta)
            .updatePackageAccountReserve(reserveId, reserveDelta, sessionId);
    }
    
    /**
     * Configure for Test Case 3: Delete Reserve
     * @param reserveId Reserve ID to delete
     * @return This builder
     */
    public GrpcCrudPayloadBuilder testCase3(long reserveId) {
        return this.deletePackageAccountReserve(reserveId);
    }
    
    // ==================== SUBMISSION ====================
    
    /**
     * Submit the batch request to the gRPC service
     * @return BatchResponse from the service
     * @throws StatusRuntimeException if gRPC call fails
     */
    public BatchResponse submit() {
        if (operations.isEmpty()) {
            throw new IllegalStateException("No operations added to batch");
        }
        
        logger.info("════════════════════════════════════════════════");
        logger.info("📤 Submitting batch to {}:{}", host, port);
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Database: {}", dbName);
        logger.info("Operations: {}", operations.size());
        logger.info("════════════════════════════════════════════════");
        
        ManagedChannel channel = null;
        try {
            // Create channel and stub
            channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
            
            BatchCrudServiceGrpc.BatchCrudServiceBlockingStub stub = 
                BatchCrudServiceGrpc.newBlockingStub(channel);
            
            // Build request
            BatchRequest request = BatchRequest.newBuilder()
                .setTransactionId(transactionId)
                .setDbName(dbName)
                .addAllOperations(operations)
                .build();
            
            // Submit and get response
            BatchResponse response = stub.executeBatch(request);
            
            // Log response
            logger.info("📥 Response received:");
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
            
            return response;
            
        } catch (StatusRuntimeException e) {
            logger.error("❌ gRPC call failed: {}", e.getStatus());
            throw e;
        } finally {
            // Cleanup
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("Error shutting down channel", e);
                }
            }
        }
    }
    
    /**
     * Submit asynchronously (returns immediately, logs results when complete)
     * @param callback Optional callback for handling response
     * @return This builder
     */
    public GrpcCrudPayloadBuilder submitAsync(ResponseCallback callback) {
        new Thread(() -> {
            try {
                BatchResponse response = submit();
                if (callback != null) {
                    callback.onSuccess(response);
                }
            } catch (Exception e) {
                if (callback != null) {
                    callback.onError(e);
                }
            }
        }).start();
        return this;
    }
    
    /**
     * Callback interface for async submission
     */
    public interface ResponseCallback {
        void onSuccess(BatchResponse response);
        void onError(Exception e);
    }
    
    // ==================== UTILITY METHODS ====================
    
    /**
     * Clear all operations (start fresh)
     * @return This builder
     */
    public GrpcCrudPayloadBuilder clear() {
        operations.clear();
        return this;
    }
    
    /**
     * Get the number of operations added
     * @return Operation count
     */
    public int getOperationCount() {
        return operations.size();
    }
    
    /**
     * Generate a new transaction ID
     * @return This builder
     */
    public GrpcCrudPayloadBuilder newTransactionId() {
        this.transactionId = "TXN_" + UUID.randomUUID().toString();
        return this;
    }
    
    // ==================== ENTITY-BASED METHODS ====================
    
    /**
     * Add a PackageAccountDelta operation (UPDATE_DELTA)
     * @param delta Proto message for PackageAccountDelta
     * @return This builder
     */
    public GrpcCrudPayloadBuilder addPackageAccountDelta(PackageAccountDelta delta) {
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT)
            .setPackageAccountDelta(delta)
            .build());
        
        logger.debug("Added UPDATE_DELTA PackageAccount: AccountID={}", delta.getAccountId());
        return this;
    }
    
    /**
     * Add a PackageAccountReserve operation (INSERT)
     * @param reserve Proto message for PackageAccountReserve
     * @return This builder
     */
    public GrpcCrudPayloadBuilder addPackageAccountReserve(com.telcobright.oltp.grpc.batch.PackageAccountReserve reserve) {
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.INSERT)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserve(reserve)
            .build());
        
        logger.debug("Added INSERT PackageAccountReserve: ID={}", reserve.getId());
        return this;
    }
    
    /**
     * Add a PackageAccountReserveDelta operation (UPDATE_DELTA)
     * @param reserveDelta Proto message for PackageAccountReserveDelta
     * @return This builder
     */
    public GrpcCrudPayloadBuilder addPackageAccountReserveDelta(com.telcobright.oltp.grpc.batch.PackageAccountReserveDelta reserveDelta) {
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.UPDATE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDelta(reserveDelta)
            .build());
        
        logger.debug("Added UPDATE_DELTA PackageAccountReserve: ReserveID={}", reserveDelta.getReserveId());
        return this;
    }
    
    /**
     * Add a PackageAccountReserve delete operation (DELETE_DELTA)
     * @param reserveId Reserve ID to delete
     * @return This builder
     */
    public GrpcCrudPayloadBuilder addPackageAccountReserveDelete(Long reserveId) {
        PackageAccountReserveDeleteDelta deleteDelta = PackageAccountReserveDeleteDelta.newBuilder()
            .setDbName(dbName)
            .setReserveId(reserveId.intValue())
            .build();
        
        operations.add(BatchOperation.newBuilder()
            .setOperationType(OperationType.DELETE_DELTA)
            .setEntityType(EntityType.PACKAGE_ACCOUNT_RESERVE)
            .setPackageAccountReserveDeleteDelta(deleteDelta)
            .build());
        
        logger.debug("Added DELETE_DELTA PackageAccountReserve: ReserveID={}", reserveId);
        return this;
    }
}