package com.telcobright.core.adapter.grpc;

import com.telcobright.api.CacheServiceProxy;
import com.telcobright.api.CacheOperationResponse;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALEntryBatch;
import com.telcobright.examples.oltp.grpc.batch.BatchRequest;
import com.telcobright.examples.oltp.grpc.batch.BatchResponse;
import com.telcobright.examples.oltp.grpc.batch.BatchCrudServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;

/**
 * gRPC Adapter for Cache Service.
 * 
 * This adapter:
 * 1. Receives gRPC requests
 * 2. Converts protobuf to domain objects (WALEntry/WALEntryBatch)
 * 3. Calls the CacheServiceProxy (the ONLY public API)
 * 4. Converts response back to protobuf
 * 
 * The adapter knows NOTHING about internal cache implementation,
 * it only knows about the two public APIs in CacheServiceProxy.
 */
@GrpcService
public class GrpcCacheAdapter extends BatchCrudServiceGrpc.BatchCrudServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(GrpcCacheAdapter.class);
    
    @Inject
    private CacheServiceProxy cacheService;
    
    @Inject
    private GrpcMessageConverter converter;
    
    /**
     * Handle batch operation request via gRPC.
     */
    @Override
    public void executeBatch(BatchRequest request, StreamObserver<BatchResponse> responseObserver) {
        logger.debug("Received gRPC batch request: txnId={}, operations={}", 
                    request.getTransactionId(), request.getOperationsCount());
        
        try {
            // Step 1: Convert gRPC request to list of WALEntry objects
            // Note: Transaction ID from client is ignored - we generate our own
            WALEntryBatch batch = converter.toBatch(request);
            
            // Step 2: Call the public API (proxy) with just the entries
            // Transaction ID will be generated internally
            CacheOperationResponse response = cacheService.performCacheOpBatch(batch.getEntries());
            
            // Step 3: Convert response to gRPC
            BatchResponse grpcResponse = converter.toGrpcResponse(response);
            
            // Step 4: Send response
            responseObserver.onNext(grpcResponse);
            responseObserver.onCompleted();
            
            logger.debug("Processed gRPC batch: success={}, processed={}", 
                        response.isSuccess(), response.getOperationsProcessed());
            
        } catch (Exception e) {
            logger.error("Error processing gRPC batch request", e);
            
            BatchResponse errorResponse = BatchResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Error: " + e.getMessage())
                .setTransactionId(request.getTransactionId())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
    
    /**
     * Handle single operation request via gRPC.
     * Note: This would require SingleOperationRequest/Response to be defined in proto files
     */
    // Placeholder - would need actual proto definitions
    /*
    public void processSingle(SingleOperationRequest request, 
                            StreamObserver<SingleOperationResponse> responseObserver) {
        // Implementation would follow same pattern as processBatch
    }
    */
    
    /**
     * Stream processing for multiple batches.
     * Note: Would need to be defined in proto file
     */
    // @Override
    public StreamObserver<BatchRequest> processStream(StreamObserver<BatchResponse> responseObserver) {
        return new StreamObserver<BatchRequest>() {
            @Override
            public void onNext(BatchRequest request) {
                try {
                    WALEntryBatch batch = converter.toBatch(request);
                    // Use the new API - just pass entries, not the whole batch
                    CacheOperationResponse response = cacheService.performCacheOpBatch(batch.getEntries());
                    BatchResponse grpcResponse = converter.toGrpcResponse(response);
                    responseObserver.onNext(grpcResponse);
                } catch (Exception e) {
                    logger.error("Error in stream processing", e);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                logger.error("Stream error", t);
                responseObserver.onError(t);
            }
            
            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}

// Note: SingleOperationRequest and SingleOperationResponse would need to be
// defined in proto files for single operation support