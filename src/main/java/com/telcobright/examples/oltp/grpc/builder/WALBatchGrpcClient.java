package com.telcobright.examples.oltp.grpc.builder;

import com.telcobright.examples.oltp.grpc.wal.*;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client for sending WALEntryBatch to server
 * Single atomic data structure for transaction processing
 */
public class WALBatchGrpcClient {
    private static final Logger logger = LoggerFactory.getLogger(WALBatchGrpcClient.class);
    
    private final String host;
    private final int port;
    
    public WALBatchGrpcClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    /**
     * Send WALEntryBatch to server
     */
    public WALBatchResponse sendWALBatch(WALEntryBatch walBatch) {
        if (walBatch == null || walBatch.isEmpty()) {
            throw new IllegalArgumentException("WALEntryBatch cannot be null or empty");
        }
        
        logger.info("════════════════════════════════════════════════");
        logger.info("📤 Sending WALEntryBatch to {}:{}", host, port);
        logger.info("Transaction ID: {}", walBatch.getTransactionId());
        logger.info("Entries: {}", walBatch.size());
        logger.info("Databases: {}", walBatch.getDatabaseNames());
        logger.info("════════════════════════════════════════════════");
        
        ManagedChannel channel = null;
        try {
            // Create channel and stub
            channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
            
            WALBatchServiceGrpc.WALBatchServiceBlockingStub stub = 
                WALBatchServiceGrpc.newBlockingStub(channel);
            
            // Convert WALEntryBatch to proto
            WALBatchRequest protoRequest = convertToProtoRequest(walBatch);
            
            // Send request
            WALBatchResponse response = stub.executeWALBatch(protoRequest);
            
            // Log response
            logger.info("📥 WALBatch Response received:");
            logger.info("  Success: {}", response.getSuccess());
            logger.info("  Message: {}", response.getMessage());
            logger.info("  Entries Processed: {}", response.getEntriesProcessed());
            logger.info("  WAL Index: {}", response.getWalIndex());
            
            if (response.hasError()) {
                logger.error("  ❌ Error: {}", response.getError().getMessage());
            }
            
            for (WALEntryResult result : response.getResultsList()) {
                logger.info("  {} Entry {}: {} {}.{} - {}", 
                    result.getSuccess() ? "✅" : "❌",
                    result.getEntryIndex(),
                    result.getOperationType(),
                    result.getDbName(),
                    result.getTableName(),
                    result.getMessage());
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
     * Convert WALEntryBatch to proto request
     */
    private WALBatchRequest convertToProtoRequest(WALEntryBatch walBatch) {
        WALBatchRequest.Builder requestBuilder = WALBatchRequest.newBuilder()
            .setTransactionId(walBatch.getTransactionId())
            .setTimestamp(walBatch.getTimestamp() != null ? walBatch.getTimestamp() : System.currentTimeMillis());
        
        // Convert each WALEntry to proto
        for (int i = 0; i < walBatch.size(); i++) {
            WALEntry entry = walBatch.get(i);
            com.telcobright.examples.oltp.grpc.wal.WALEntry protoEntry = convertToProtoEntry(entry);
            requestBuilder.addEntries(protoEntry);
        }
        
        return requestBuilder.build();
    }
    
    /**
     * Convert WALEntry to proto entry
     */
    private com.telcobright.examples.oltp.grpc.wal.WALEntry convertToProtoEntry(WALEntry walEntry) {
        com.telcobright.examples.oltp.grpc.wal.WALEntry.Builder entryBuilder = 
            com.telcobright.examples.oltp.grpc.wal.WALEntry.newBuilder()
                .setDbName(walEntry.getDbName())
                .setTableName(walEntry.getTableName())
                .setOperationType(convertToProtoOperationType(walEntry.getOperationType()))
                .setTimestamp(walEntry.getTimestamp() != null ? walEntry.getTimestamp() : System.currentTimeMillis());
        
        // Convert data map to proto
        Map<String, WALValue> protoData = new HashMap<>();
        for (Map.Entry<String, Object> dataEntry : walEntry.getData().entrySet()) {
            WALValue protoValue = convertToProtoValue(dataEntry.getValue());
            if (protoValue != null) {
                protoData.put(dataEntry.getKey(), protoValue);
            }
        }
        
        entryBuilder.putAllData(protoData);
        return entryBuilder.build();
    }
    
    /**
     * Convert WAL operation type to proto
     */
    private WALOperationType convertToProtoOperationType(WALEntry.OperationType walType) {
        switch (walType) {
            case INSERT: return WALOperationType.INSERT;
            case UPDATE: return WALOperationType.UPDATE;
            case DELETE: return WALOperationType.DELETE;
            case UPSERT: return WALOperationType.UPSERT;
            case BATCH: return WALOperationType.BATCH;
            default: return WALOperationType.UPDATE;
        }
    }
    
    /**
     * Convert Java object to proto value
     */
    private WALValue convertToProtoValue(Object value) {
        if (value == null) {
            return null;
        }
        
        WALValue.Builder valueBuilder = WALValue.newBuilder();
        
        if (value instanceof String) {
            valueBuilder.setStringValue((String) value);
        } else if (value instanceof Long) {
            valueBuilder.setLongValue((Long) value);
        } else if (value instanceof Integer) {
            valueBuilder.setIntValue((Integer) value);
        } else if (value instanceof Double) {
            valueBuilder.setDoubleValue((Double) value);
        } else if (value instanceof Boolean) {
            valueBuilder.setBoolValue((Boolean) value);
        } else if (value instanceof BigDecimal) {
            valueBuilder.setDecimalValue(((BigDecimal) value).toString());
        } else {
            // Fallback to string representation
            valueBuilder.setStringValue(value.toString());
        }
        
        return valueBuilder.build();
    }
    
    /**
     * Static factory method
     */
    public static WALBatchGrpcClient create(String host, int port) {
        return new WALBatchGrpcClient(host, port);
    }
    
    /**
     * Convenience method with default localhost:9000
     */
    public static WALBatchGrpcClient create() {
        return new WALBatchGrpcClient("localhost", 9000);
    }
    
    // ==================== Builder Pattern for WALEntryBatch Creation ====================
    
    /**
     * Builder for creating WALEntryBatch with fluent API
     */
    public static class WALBatchBuilder {
        private final WALEntryBatch.Builder batchBuilder;
        
        public WALBatchBuilder() {
            this.batchBuilder = WALEntryBatch.builder();
        }
        
        public WALBatchBuilder transactionId(String transactionId) {
            batchBuilder.transactionId(transactionId);
            return this;
        }
        
        public WALBatchBuilder generateTransactionId() {
            batchBuilder.generateTransactionId();
            return this;
        }
        
        // PackageAccount operations
        public WALBatchBuilder updatePackageAccount(String dbName, Long accountId, String amount) {
            WALEntry entry = WALEntry.builder()
                .dbName(dbName)
                .tableName("packageaccount")
                .operationType(WALEntry.OperationType.UPDATE)
                .withData("accountId", accountId)
                .withData("amount", new BigDecimal(amount))
                .build();
            batchBuilder.addEntry(entry);
            return this;
        }
        
        public WALBatchBuilder insertPackageAccount(String dbName, Long accountId, Long packagePurchaseId, 
                                                  String name, String balanceAfter, String uom) {
            WALEntry entry = WALEntry.builder()
                .dbName(dbName)
                .tableName("packageaccount")
                .operationType(WALEntry.OperationType.INSERT)
                .withData("id", accountId)
                .withData("packagePurchaseId", packagePurchaseId)
                .withData("name", name)
                .withData("balanceAfter", new BigDecimal(balanceAfter))
                .withData("uom", uom)
                .withData("isSelected", true)
                .build();
            batchBuilder.addEntry(entry);
            return this;
        }
        
        // PackageAccountReserve operations
        public WALBatchBuilder insertPackageAccountReserve(String dbName, Long reserveId, Long packageAccountId,
                                                          String reserveAmount, String sessionId) {
            WALEntry entry = WALEntry.builder()
                .dbName(dbName)
                .tableName("packageaccountreserve")
                .operationType(WALEntry.OperationType.INSERT)
                .withData("id", reserveId)
                .withData("packageAccountId", packageAccountId)
                .withData("sessionId", sessionId)
                .withData("reservedAmount", new BigDecimal(reserveAmount))
                .withData("currentReserve", new BigDecimal(reserveAmount))
                .withData("status", "RESERVED")
                .build();
            batchBuilder.addEntry(entry);
            return this;
        }
        
        public WALBatchBuilder updatePackageAccountReserve(String dbName, Long reserveId, 
                                                         String deltaAmount, String sessionId) {
            WALEntry entry = WALEntry.builder()
                .dbName(dbName)
                .tableName("packageaccountreserve")
                .operationType(WALEntry.OperationType.UPDATE)
                .withData("reserveId", reserveId)
                .withData("amount", new BigDecimal(deltaAmount))
                .withData("sessionId", sessionId)
                .build();
            batchBuilder.addEntry(entry);
            return this;
        }
        
        public WALBatchBuilder deletePackageAccountReserve(String dbName, Long reserveId) {
            WALEntry entry = WALEntry.builder()
                .dbName(dbName)
                .tableName("packageaccountreserve")
                .operationType(WALEntry.OperationType.DELETE)
                .withData("reserveId", reserveId)
                .build();
            batchBuilder.addEntry(entry);
            return this;
        }
        
        // Generic entry addition
        public WALBatchBuilder addEntry(WALEntry entry) {
            batchBuilder.addEntry(entry);
            return this;
        }
        
        public WALEntryBatch build() {
            return batchBuilder.build();
        }
    }
    
    /**
     * Start building a WALEntryBatch
     */
    public static WALBatchBuilder batchBuilder() {
        return new WALBatchBuilder();
    }
}