package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.grpc.wal.*;
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
 * gRPC service for WALEntryBatch operations - single atomic data structure
 */
@GrpcService
public class WALBatchGrpcService extends WALBatchServiceGrpc.WALBatchServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(WALBatchGrpcService.class);
    private static final String SERVER_LOG_FILE = "server-received-wal-batches.log";
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // WALWriter will be null in test mode - we'll handle this gracefully
    private WALWriter walWriter = null; // Not injected in test mode
    
    @Override
    public void executeWALBatch(WALBatchRequest request, StreamObserver<WALBatchResponse> responseObserver) {
        String transactionId = request.getTransactionId();
        long timestamp = request.getTimestamp();
        List<com.telcobright.oltp.grpc.wal.WALEntry> protoEntries = request.getEntriesList();
        
        logger.info("════════════════════════════════════════════════════════");
        logger.info("📥 WALBatch Request Received");
        logger.info("Transaction ID: {}", transactionId);
        logger.info("Timestamp: {}", timestamp);
        logger.info("Entries count: {}", protoEntries.size());
        logger.info("════════════════════════════════════════════════════════");
        
        try {
            // Convert proto WALBatchRequest to WALEntryBatch
            WALEntryBatch walBatch = convertProtoToWALBatch(request);
            
            // Log received WALBatch to file for verification
            logReceivedWALBatch(walBatch);
            
            // Process the WALEntryBatch atomically
            List<WALEntryResult> results = new ArrayList<>();
            boolean overallSuccess = true;
            String errorMessage = "";
            long walIndex = -1;
            
            try {
                // Write to WAL atomically
                if (walWriter != null) {
                    walIndex = walWriter.write(walBatch);
                    logger.info("✅ WALBatch written to WAL at index: {}", walIndex);
                }
                
                // Process each entry for database operations
                for (int i = 0; i < walBatch.size(); i++) {
                    WALEntry entry = walBatch.get(i);
                    WALEntryResult result = processWALEntry(entry, i);
                    results.add(result);
                    
                    if (!result.getSuccess()) {
                        overallSuccess = false;
                        if (errorMessage.isEmpty()) {
                            errorMessage = result.getMessage();
                        }
                    }
                }
                
            } catch (Exception e) {
                overallSuccess = false;
                errorMessage = "Failed to process WALBatch: " + e.getMessage();
                logger.error("❌ WALBatch processing failed", e);
            }
            
            // Build response
            WALBatchResponse.Builder responseBuilder = WALBatchResponse.newBuilder()
                .setSuccess(overallSuccess)
                .setTransactionId(transactionId)
                .setEntriesProcessed(results.size())
                .setWalIndex(walIndex)
                .addAllResults(results);
            
            if (overallSuccess) {
                responseBuilder.setMessage("WALBatch processed successfully");
                logger.info("✅ WALBatch completed successfully: {} entries processed", results.size());
            } else {
                responseBuilder
                    .setMessage(errorMessage)
                    .setError(WALErrorDetails.newBuilder()
                        .setCode("PROCESSING_ERROR")
                        .setMessage(errorMessage)
                        .build());
                logger.error("❌ WALBatch completed with errors: {}", errorMessage);
            }
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            logger.error("❌ Critical error processing WALBatch", e);
            
            WALBatchResponse errorResponse = WALBatchResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Critical error: " + e.getMessage())
                .setTransactionId(transactionId)
                .setError(WALErrorDetails.newBuilder()
                    .setCode("CRITICAL_ERROR")
                    .setMessage(e.getMessage())
                    .build())
                .build();
                
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * Convert proto WALBatchRequest to WALEntryBatch
     */
    private WALEntryBatch convertProtoToWALBatch(WALBatchRequest protoRequest) {
        WALEntryBatch.Builder batchBuilder = WALEntryBatch.builder()
            .transactionId(protoRequest.getTransactionId())
            .timestamp(protoRequest.getTimestamp());
            
        for (com.telcobright.oltp.grpc.wal.WALEntry protoEntry : protoRequest.getEntriesList()) {
            WALEntry walEntry = convertProtoToWALEntry(protoEntry);
            if (walEntry != null) {
                batchBuilder.addEntry(walEntry);
            }
        }
        
        return batchBuilder.build();
    }
    
    /**
     * Convert proto WALEntry to WALEntry
     */
    private WALEntry convertProtoToWALEntry(com.telcobright.oltp.grpc.wal.WALEntry protoEntry) {
        WALEntry.Builder entryBuilder = WALEntry.builder()
            .dbName(protoEntry.getDbName())
            .tableName(protoEntry.getTableName())
            .operationType(convertProtoOperationType(protoEntry.getOperationType()));
            
        // Convert proto data map to WALEntry data
        Map<String, Object> data = new HashMap<>();
        for (Map.Entry<String, WALValue> protoData : protoEntry.getDataMap().entrySet()) {
            Object value = convertProtoValue(protoData.getValue());
            if (value != null) {
                data.put(protoData.getKey(), value);
            }
        }
        
        entryBuilder.data(data);
        return entryBuilder.build();
    }
    
    /**
     * Convert proto operation type to WAL operation type
     */
    private WALEntry.OperationType convertProtoOperationType(WALOperationType protoType) {
        switch (protoType) {
            case INSERT: return WALEntry.OperationType.INSERT;
            case UPDATE: return WALEntry.OperationType.UPDATE;
            case DELETE: return WALEntry.OperationType.DELETE;
            case UPSERT: return WALEntry.OperationType.UPSERT;
            case BATCH: return WALEntry.OperationType.BATCH;
            default: return WALEntry.OperationType.UPDATE;
        }
    }
    
    /**
     * Convert proto value to Java object
     */
    private Object convertProtoValue(WALValue protoValue) {
        switch (protoValue.getValueCase()) {
            case STRING_VALUE: return protoValue.getStringValue();
            case LONG_VALUE: return protoValue.getLongValue();
            case INT_VALUE: return (long) protoValue.getIntValue(); // Convert to Long for consistency
            case DOUBLE_VALUE: return protoValue.getDoubleValue();
            case BOOL_VALUE: return protoValue.getBoolValue();
            case DECIMAL_VALUE: return new BigDecimal(protoValue.getDecimalValue());
            default: return null;
        }
    }
    
    /**
     * Process individual WAL entry (simulate database operations)
     */
    private WALEntryResult processWALEntry(WALEntry entry, int index) {
        try {
            logger.info("  Processing WALEntry[{}]: {} {} on {}.{}", 
                index, entry.getOperationType(), entry.getTableName(), 
                entry.getDbName(), entry.getTableName());
            
            // Simulate database processing based on table and operation
            switch (entry.getTableName()) {
                case "packageaccount":
                    return processPackageAccountEntry(entry, index);
                case "packageaccountreserve":
                    return processPackageAccountReserveEntry(entry, index);
                default:
                    return createErrorResult(index, entry, "Unknown table: " + entry.getTableName());
            }
            
        } catch (Exception e) {
            logger.error("Error processing WALEntry[{}]", index, e);
            return createErrorResult(index, entry, "Processing error: " + e.getMessage());
        }
    }
    
    /**
     * Process PackageAccount WAL entry
     */
    private WALEntryResult processPackageAccountEntry(WALEntry entry, int index) {
        switch (entry.getOperationType()) {
            case INSERT:
                logger.info("    📝 INSERT PackageAccount: {}", entry.getData());
                break;
            case UPDATE:
                Long accountId = (Long) entry.get("accountId");
                BigDecimal amount = (BigDecimal) entry.get("amount");
                logger.info("    📝 UPDATE PackageAccount: accountId={}, amount={}", accountId, amount);
                break;
            case DELETE:
                logger.info("    📝 DELETE PackageAccount: {}", entry.getData());
                break;
            default:
                return createErrorResult(index, entry, "Unsupported operation for PackageAccount: " + entry.getOperationType());
        }
        
        return createSuccessResult(index, entry, "PackageAccount processed successfully");
    }
    
    /**
     * Process PackageAccountReserve WAL entry
     */
    private WALEntryResult processPackageAccountReserveEntry(WALEntry entry, int index) {
        switch (entry.getOperationType()) {
            case INSERT:
                logger.info("    📝 INSERT PackageAccountReserve: {}", entry.getData());
                break;
            case UPDATE:
                Long reserveId = (Long) entry.get("reserveId");
                BigDecimal amount = (BigDecimal) entry.get("amount");
                logger.info("    📝 UPDATE PackageAccountReserve: reserveId={}, amount={}", reserveId, amount);
                break;
            case DELETE:
                logger.info("    📝 DELETE PackageAccountReserve: {}", entry.getData());
                break;
            default:
                return createErrorResult(index, entry, "Unsupported operation for PackageAccountReserve: " + entry.getOperationType());
        }
        
        return createSuccessResult(index, entry, "PackageAccountReserve processed successfully");
    }
    
    /**
     * Create success result
     */
    private WALEntryResult createSuccessResult(int index, WALEntry entry, String message) {
        return WALEntryResult.newBuilder()
            .setEntryIndex(index)
            .setSuccess(true)
            .setMessage(message)
            .setDbName(entry.getDbName())
            .setTableName(entry.getTableName())
            .setOperationType(convertToProtoOperationType(entry.getOperationType()))
            .build();
    }
    
    /**
     * Create error result
     */
    private WALEntryResult createErrorResult(int index, WALEntry entry, String errorMessage) {
        return WALEntryResult.newBuilder()
            .setEntryIndex(index)
            .setSuccess(false)
            .setMessage(errorMessage)
            .setDbName(entry.getDbName())
            .setTableName(entry.getTableName())
            .setOperationType(convertToProtoOperationType(entry.getOperationType()))
            .build();
    }
    
    /**
     * Convert WAL operation type to proto operation type
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
     * Log received WALBatch to file for verification
     */
    private void logReceivedWALBatch(WALEntryBatch walBatch) {
        try {
            StringBuilder batchLog = new StringBuilder();
            batchLog.append("RECEIVED_WAL_BATCH|");
            batchLog.append(TIMESTAMP_FORMAT.format(LocalDateTime.now())).append("|");
            batchLog.append(walBatch.getTransactionId()).append("|");
            batchLog.append(walBatch.size()).append("|");
            
            // Get all database names
            List<String> databases = walBatch.getDatabaseNames();
            batchLog.append("databases=").append(String.join(",", databases)).append("|");
            
            // Log each entry
            for (int i = 0; i < walBatch.size(); i++) {
                WALEntry entry = walBatch.get(i);
                batchLog.append("ENTRY").append(i + 1).append(":");
                batchLog.append(entry.getOperationType()).append("|");
                batchLog.append(entry.getDbName()).append(".").append(entry.getTableName()).append("|");
                batchLog.append(entry.getData().toString());
                
                if (i < walBatch.size() - 1) {
                    batchLog.append("|");
                }
            }
            
            try (FileWriter writer = new FileWriter(SERVER_LOG_FILE, true)) {
                writer.write(batchLog.toString() + "\n");
                writer.flush();
            }
            
            logger.info("📝 Logged received WALBatch to: {}", SERVER_LOG_FILE);
            
        } catch (IOException e) {
            logger.error("Failed to log WALBatch", e);
        }
    }
}