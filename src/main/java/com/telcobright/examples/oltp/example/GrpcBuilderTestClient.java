package com.telcobright.examples.oltp.example;

import com.telcobright.examples.oltp.grpc.batch.BatchResponse;
import com.telcobright.examples.oltp.grpc.builder.GrpcCrudPayloadBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * Test client demonstrating the use of GrpcCrudPayloadBuilder for all test cases
 */
public class GrpcBuilderTestClient {
    private static final Logger logger = LoggerFactory.getLogger(GrpcBuilderTestClient.class);
    
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9000;
        
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        
        logger.info("╔══════════════════════════════════════════════╗");
        logger.info("║   Testing GrpcCrudPayloadBuilder             ║");
        logger.info("║   Server: {}:{}                              ║", host, port);
        logger.info("╚══════════════════════════════════════════════╝");
        
        try {
            // Test Case 1: Update PackageAccount + Insert Reserve
            testCase1(host, port);
            Thread.sleep(2000);
            
//            // Test Case 2: Update PackageAccount + Update Reserve
//            testCase2(host, port);
//            Thread.sleep(2000);
//
//            // Test Case 3: Delete Reserve
//            testCase3(host, port);
//            Thread.sleep(2000);
//
//            // Bonus: Demonstrate chaining multiple operations
//            testComplexBatch(host, port);
            
            logger.info("\n✅ All tests completed successfully!");
            
        } catch (Exception e) {
            logger.error("❌ Test failed: ", e);
        }
    }
    
    /**
     * Test Case 1: Update PackageAccount with delta + Insert new Reserve
     */
    private static void testCase1(String host, int port) {
        logger.info("\n┌─────────────────────────────────────────────┐");
        logger.info("│  TEST CASE 1: Update Account + Insert Reserve│");
        logger.info("└─────────────────────────────────────────────┘");
        
        BatchResponse response = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("telcobright")
            .withTransactionId("CASE1_" + UUID.randomUUID())
            // op2: Update PackageAccount delta
            .updatePackageAccount(199, "6")
            // op3: Insert PackageAccountReserve
            .insertPackageAccountReserve(4001, 199, "6.00", "SESSION_CASE1_" + UUID.randomUUID())
            .submit();
        
        logger.info("Response: Success={}, Message={}", response.getSuccess(), response.getMessage());
    }
    
    /**
     * Test Case 2: Update PackageAccount + Update Reserve with deltas
     */
    private static void testCase2(String host, int port) {
        logger.info("\n┌─────────────────────────────────────────────┐");
        logger.info("│  TEST CASE 2: Update Account + Update Reserve│");
        logger.info("└─────────────────────────────────────────────┘");
        
        BatchResponse response = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .withTransactionId("CASE2_" + UUID.randomUUID())
            // op2: Update PackageAccount delta
            .updatePackageAccount(2001, "-25.75")
            // op4: Update PackageAccountReserve delta (release)
            .updatePackageAccountReserve(4001, "-15.00", "SESSION_CASE2_" + UUID.randomUUID())
            .submit();
        
        logger.info("Response: Success={}, Message={}", response.getSuccess(), response.getMessage());
    }
    
    /**
     * Test Case 3: Delete PackageAccountReserve
     */
    private static void testCase3(String host, int port) {
        logger.info("\n┌─────────────────────────────────────────────┐");
        logger.info("│  TEST CASE 3: Delete Reserve                 │");
        logger.info("└─────────────────────────────────────────────┘");
        
        BatchResponse response = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .withTransactionId("CASE3_" + UUID.randomUUID())
            // op5: Delete PackageAccountReserve
            .deletePackageAccountReserve(5555)
            .submit();
        
        logger.info("Response: Success={}, Message={}", response.getSuccess(), response.getMessage());
    }
    
    /**
     * Demonstrate using convenience methods for test cases
     */
    private static void testComplexBatch(String host, int port) {
        logger.info("\n┌─────────────────────────────────────────────┐");
        logger.info("│  COMPLEX BATCH: Multiple Operations          │");
        logger.info("└─────────────────────────────────────────────┘");
        
        BatchResponse response = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .withTransactionId("COMPLEX_" + UUID.randomUUID())
            // Insert a new account
            .insertPackageAccount(9001, 5001, "Premium Package", "1000.00", "GB")
            // Update multiple accounts
            .updatePackageAccount(9001, "-50.00")
            .updatePackageAccount(9002, "100.00")
            // Create multiple reserves
            .insertPackageAccountReserve(7001, 9001, "25.00", "SESSION_A")
            .insertPackageAccountReserve(7002, 9001, "15.00", "SESSION_B")
            // Update a reserve
            .updatePackageAccountReserve(7001, "-10.00", "SESSION_A_UPDATE")
            // Delete old reserves
            .deletePackageAccountReserve(6001)
            .deletePackageAccountReserve(6002)
            .submit();
        
        logger.info("Complex batch with {} operations: Success={}", 
            response.getOperationsProcessed(), response.getSuccess());
    }
    
    /**
     * Demonstrate using the convenience test case methods
     */
    private static void testConvenienceMethods(String host, int port) {
        logger.info("\n┌─────────────────────────────────────────────┐");
        logger.info("│  Using Convenience Methods                   │");
        logger.info("└─────────────────────────────────────────────┘");
        
        // Test Case 1 using convenience method
        BatchResponse response1 = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .testCase1(1001, "75.50", 4001, "30.00", "SESSION_001")
            .submit();
        
        // Test Case 2 using convenience method
        BatchResponse response2 = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .testCase2(2001, "-25.75", 4001, "-15.00", "SESSION_002")
            .submit();
        
        // Test Case 3 using convenience method
        BatchResponse response3 = GrpcCrudPayloadBuilder.create(host, port)
            .withDatabase("res_1")
            .testCase3(5555)
            .submit();
        
        logger.info("Convenience methods test completed");
    }
}