package com.telcobright.core.adapter.grpc;

import com.telcobright.api.CacheOperationResponse;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.examples.oltp.grpc.batch.BatchRequest;
import com.telcobright.examples.oltp.grpc.batch.BatchResponse;
import com.telcobright.examples.oltp.grpc.batch.BatchOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter between gRPC protobuf messages and domain objects.
 * Isolates the gRPC-specific conversion logic.
 */
@ApplicationScoped
public class GrpcMessageConverter {
    private static final Logger logger = LoggerFactory.getLogger(GrpcMessageConverter.class);
    
    /**
     * Convert gRPC BatchRequest to WALEntryBatch.
     */
    public WALEntryBatch toBatch(BatchRequest request) {
        WALEntryBatch.Builder batchBuilder = WALEntryBatch.builder()
            .transactionId(request.getTransactionId());
        
        // Convert each operation to WALEntry
        for (BatchOperation op : request.getOperationsList()) {
            WALEntry entry = toEntry(op);
            batchBuilder.addEntry(entry);
        }
        
        return batchBuilder.build();
    }
    
    /**
     * Convert gRPC Operation to WALEntry.
     */
    public WALEntry toEntry(BatchOperation op) {
        // This is a placeholder - actual implementation would map
        // BatchOperation's payload fields to WALEntry
        WALEntry.Builder entryBuilder = WALEntry.builder()
            .dbName("default") // Would come from operation context
            .tableName("entity") // Would be derived from entity_type
            .operationType(convertOperationType(op.getOperationType().name()));
        
        // In real implementation, would extract data from the payload oneof
        Map<String, Object> data = new HashMap<>();
        // data would be populated from op.getPackageAccount(), etc.
        entryBuilder.data(data);
        
        return entryBuilder.build();
    }
    
    /**
     * Convert single operation request to WALEntry.
     * Note: SingleOperationRequest would need to be defined in proto files
     */
    // Placeholder - would need actual proto definition
    /*
    public WALEntry toEntry(SingleOperationRequest request) {
        return WALEntry.builder()
            .tableName(request.getTableName())
            .operationType(WALEntry.OperationType.valueOf(request.getOperationType()))
            .build();
    }
    */
    
    /**
     * Convert CacheOperationResponse to gRPC BatchResponse.
     */
    public BatchResponse toGrpcResponse(CacheOperationResponse response) {
        BatchResponse.Builder builder = BatchResponse.newBuilder()
            .setSuccess(response.isSuccess())
            .setTransactionId(response.getTransactionId())
            .setOperationsProcessed(response.getOperationsProcessed());
        
        if (response.getErrorMessage() != null) {
            builder.setMessage(response.getErrorMessage());
        } else {
            builder.setMessage("Success");
        }
        
        return builder.build();
    }
    
    /**
     * Convert CacheOperationResponse to gRPC SingleOperationResponse.
     * Note: SingleOperationResponse would need to be defined in proto files
     */
    // Placeholder - would need actual proto definition
    /*
    public SingleOperationResponse toGrpcSingleResponse(CacheOperationResponse response) {
        return SingleOperationResponse.newBuilder()
            .setSuccess(response.isSuccess())
            .setMessage(response.getErrorMessage() != null ? 
                       response.getErrorMessage() : "Success")
            .build();
    }
    */
    
    /**
     * Convert gRPC operation type to domain operation type.
     */
    private WALEntry.OperationType convertOperationType(String grpcType) {
        switch (grpcType.toUpperCase()) {
            case "INSERT":
                return WALEntry.OperationType.INSERT;
            case "UPDATE":
                return WALEntry.OperationType.UPDATE;
            case "DELETE":
                return WALEntry.OperationType.DELETE;
            case "UPSERT":
                return WALEntry.OperationType.UPSERT;
            default:
                logger.warn("Unknown operation type: {}, defaulting to UPSERT", grpcType);
                return WALEntry.OperationType.UPSERT;
        }
    }
}