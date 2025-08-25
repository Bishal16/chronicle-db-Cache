import com.telcobright.oltp.grpc.batch.BatchResponse;
import com.telcobright.oltp.grpc.builder.GrpcCrudPayloadBuilder;

public class QuickTest {
    public static void main(String[] args) {
        try {
            System.out.println("=== QUICK TEST ===");
            
            BatchResponse response = GrpcCrudPayloadBuilder.create("localhost", 9000)
                .withDatabase("res_1")
                .withTransactionId("QUICK_TEST")
                .updatePackageAccount(1001, "75.50")
                .insertPackageAccountReserve(4001, 1001, "30.00", "SESSION_001")
                .submit();
            
            System.out.println("Response: " + response.getSuccess());
            System.out.println("Message: " + response.getMessage());
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}