package com.telcobright.oltp.example;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Standalone server for testing WALBatch gRPC transmission
 * Uses WALBatchGrpcService for atomic WALEntryBatch operations
 */
@QuarkusMain
public class WALBatchTestServer implements QuarkusApplication {
    private static final Logger logger = LoggerFactory.getLogger(WALBatchTestServer.class);
    
    @Override
    public int run(String... args) throws Exception {
        logger.info("╔══════════════════════════════════════════════════════════════╗");
        logger.info("║                WALBatch Test Server Starting                 ║");
        logger.info("║                    gRPC Port: 9000                           ║");
        logger.info("║              Service: WALBatchGrpcService                    ║");
        logger.info("╚══════════════════════════════════════════════════════════════╝");
        
        logger.info("🚀 Server is ready to receive WALEntryBatch requests");
        logger.info("📁 Server logs will be written to: server-received-wal-batches.log");
        logger.info("🔄 Press Ctrl+C to stop the server");
        
        Quarkus.waitForExit();
        return 0;
    }
    
    public static void main(String... args) {
        Quarkus.run(WALBatchTestServer.class, args);
    }
}