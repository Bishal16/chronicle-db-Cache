package com.telcobright.examples.oltp.example;

import com.telcobright.examples.oltp.grpc.batch.BatchResponse;
import com.telcobright.examples.oltp.grpc.builder.GrpcCrudPayloadBuilder;

/**
 * Simple standalone test of the builder
 */
public class SimpleBuilderTest {
    
    public static void main(String[] args) {
        System.out.println("Testing GrpcCrudPayloadBuilder...");
        
        try {
            // Test Case 1
            System.out.println("\n=== TEST CASE 1 ===");
            BatchResponse response1 = GrpcCrudPayloadBuilder.create("localhost", 9000)
                .withDatabase("res_1")
                .withTransactionId("TEST1")
                .updatePackageAccount(1001, "75.50")
                .insertPackageAccountReserve(4001, 1001, "30.00", "SESSION_001")
                .submit();
            System.out.println("Case 1 Success: " + response1.getSuccess());
            
            Thread.sleep(1000);
            
            // Test Case 2
            System.out.println("\n=== TEST CASE 2 ===");
            BatchResponse response2 = GrpcCrudPayloadBuilder.create()
                .withDatabase("res_1")
                .withTransactionId("TEST2")
                .updatePackageAccount(2001, "-25.75")
                .updatePackageAccountReserve(4001, "-15.00", "SESSION_002")
                .submit();
            System.out.println("Case 2 Success: " + response2.getSuccess());
            
            Thread.sleep(1000);
            
            // Test Case 3
            System.out.println("\n=== TEST CASE 3 ===");
            BatchResponse response3 = GrpcCrudPayloadBuilder.create()
                .withDatabase("res_1")
                .withTransactionId("TEST3")
                .deletePackageAccountReserve(5555)
                .submit();
            System.out.println("Case 3 Success: " + response3.getSuccess());
            
            System.out.println("\n✅ All tests completed!");
            
        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}