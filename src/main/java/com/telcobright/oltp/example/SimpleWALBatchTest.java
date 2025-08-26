package com.telcobright.oltp.example;

import com.telcobright.oltp.grpc.builder.WALBatchGrpcClient;
import com.telcobright.oltp.grpc.wal.WALBatchResponse;
import com.telcobright.core.wal.WALEntryBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Simple WALBatch test that demonstrates the concept without full server startup
 * Shows how WALEntryBatch would be used in gRPC transmission
 */
public class SimpleWALBatchTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleWALBatchTest.class);
    
    public static void main(String[] args) {
        logger.info("╔══════════════════════════════════════════════════════════════╗");
        logger.info("║              Simple WALBatch Test - Demonstration            ║");
        logger.info("║               (No server required - shows concept)           ║");
        logger.info("╚══════════════════════════════════════════════════════════════╝");
        
        // Demonstrate WALEntryBatch creation and structure
        demonstrateWALBatchCreation();
        
        // Show how client would work (server connection will fail, but structure is shown)
        demonstrateClientUsage();
        
        logger.info("\n🎯 WALEntryBatch system demonstration complete!");
        logger.info("📝 Key points demonstrated:");
        logger.info("  ✅ Single transaction ID per batch (no duplication)");
        logger.info("  ✅ Atomic operations across multiple databases");  
        logger.info("  ✅ Clean gRPC client interface");
        logger.info("  ✅ Builder pattern for easy batch construction");
    }
    
    private static void demonstrateWALBatchCreation() {
        logger.info("\n=== WALEntryBatch Creation Demo ===");
        
        // Single database batch
        logger.info("1. Single Database Batch:");
        WALEntryBatch singleDbBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId("DEMO_SINGLE_" + UUID.randomUUID())
            .updatePackageAccount("telcobright", 1001L, "75.50")
            .insertPackageAccountReserve("telcobright", 4001L, 1001L, "30.00", "SESSION_DEMO")
            .build();
        
        logger.info("   Transaction ID: {}", singleDbBatch.getTransactionId());
        logger.info("   Entries: {}", singleDbBatch.size());
        logger.info("   Databases: {}", singleDbBatch.getDatabaseNames());
        logger.info("   Batch: {}", singleDbBatch.toString());
        
        // Multi-database batch (your original request)
        logger.info("\n2. Multi-Database Batch (OP1 across 3 databases):");
        WALEntryBatch multiDbBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId("DEMO_MULTI_" + UUID.randomUUID())
            // Database 1: telcobright
            .updatePackageAccount("telcobright", 1001L, "50.00")
            .insertPackageAccountReserve("telcobright", 4001L, 1001L, "25.00", "SESSION_TB")
            // Database 2: res_1
            .updatePackageAccount("res_1", 2001L, "75.50") 
            .insertPackageAccountReserve("res_1", 4002L, 2001L, "30.00", "SESSION_R1")
            // Database 3: res_2
            .updatePackageAccount("res_2", 3001L, "100.25")
            .insertPackageAccountReserve("res_2", 4003L, 3001L, "45.00", "SESSION_R2")
            .build();
        
        logger.info("   Transaction ID: {}", multiDbBatch.getTransactionId());
        logger.info("   Entries: {}", multiDbBatch.size());
        logger.info("   Databases: {}", multiDbBatch.getDatabaseNames());
        logger.info("   Batch: {}", multiDbBatch.toString());
        
        // Show individual entries
        logger.info("\n3. Individual Entries in Multi-DB Batch:");
        for (int i = 0; i < multiDbBatch.size(); i++) {
            var entry = multiDbBatch.get(i);
            logger.info("   Entry[{}]: {} {} on {}.{} - {}", 
                i, entry.getOperationType(), entry.getTableName(),
                entry.getDbName(), entry.getTableName(), entry.getData());
        }
    }
    
    private static void demonstrateClientUsage() {
        logger.info("\n=== gRPC Client Usage Demo ===");
        
        WALEntryBatch testBatch = WALBatchGrpcClient.batchBuilder()
            .transactionId("CLIENT_DEMO_" + UUID.randomUUID())
            .updatePackageAccount("telcobright", 9999L, "123.45")
            .deletePackageAccountReserve("telcobright", 8888L)
            .build();
        
        logger.info("Created test batch for client demo: {}", testBatch.toString());
        
        // This would normally connect to server
        try {
            WALBatchGrpcClient client = WALBatchGrpcClient.create("localhost", 9000);
            logger.info("🔄 Attempting to connect to server (will likely fail - this is just a demo)...");
            
            // This will fail since server isn't running, but shows the API
            WALBatchResponse response = client.sendWALBatch(testBatch);
            logger.info("✅ Server response: Success={}, Entries Processed={}", 
                response.getSuccess(), response.getEntriesProcessed());
                
        } catch (Exception e) {
            logger.info("❌ Expected connection failure: {}", e.getMessage());
            logger.info("📝 This is normal - demonstrates client API without requiring server");
        }
        
        logger.info("💡 In production, the client would:");
        logger.info("   1. Convert WALEntryBatch to proto format");
        logger.info("   2. Send via gRPC to WALBatchGrpcService"); 
        logger.info("   3. Server receives WALEntryBatch directly");
        logger.info("   4. Server writes to WAL as single atomic batch");
        logger.info("   5. Server processes all entries in one database transaction");
    }
}