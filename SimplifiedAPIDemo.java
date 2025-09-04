import com.telcobright.api.CacheServiceProxy;
import com.telcobright.api.CacheOperationResponse;
import com.telcobright.core.wal.WALEntry;
import java.util.*;

/**
 * Demonstration of the SIMPLIFIED Cache API.
 * 
 * KEY CHANGE: Only ONE method for all operations - performCacheOpBatch(List<WALEntry>)
 * 
 * Benefits:
 * 1. Simpler API - less confusion about which method to use
 * 2. Always atomic - all operations are batch operations
 * 3. Less error-prone - no special cases for single vs batch
 * 4. Consistent behavior - same code path for all operations
 */
public class SimplifiedAPIDemo {
    
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("SIMPLIFIED CACHE API DEMONSTRATION");
        System.out.println("========================================\n");
        
        // Mock cache service
        CacheServiceProxy cacheService = new CacheServiceProxy() {
            @Override
            public CacheOperationResponse performCacheOpBatch(List<WALEntry> entries) {
                System.out.println("  → Processing " + entries.size() + 
                    " entries as atomic batch");
                return CacheOperationResponse.success("TXN_" + System.currentTimeMillis(), 
                    entries.size());
            }
        };
        
        System.out.println("BEFORE (Old API - Confusing):");
        System.out.println("  performCacheOpSingle(WALEntry) - for single entries");
        System.out.println("  performCacheOpBatch(List<WALEntry>) - for multiple entries");
        System.out.println("  → Users had to choose which method to use\n");
        
        System.out.println("AFTER (New API - Simple):");
        System.out.println("  performCacheOpBatch(List<WALEntry>) - for ALL operations");
        System.out.println("  → ONE method for everything!\n");
        
        System.out.println("========================================\n");
        
        // Example 1: Single Entry Operation
        System.out.println("1. SINGLE ENTRY OPERATION:");
        System.out.println("   Old way: performCacheOpSingle(entry)");
        System.out.println("   New way: performCacheOpBatch(List.of(entry))\n");
        
        Map<String, Object> singleData = new HashMap<>();
        singleData.put("id", 1L);
        singleData.put("name", "John Doe");
        singleData.put("email", "john@example.com");
        
        WALEntry singleEntry = new WALEntry.Builder()
            .dbName("mydb")
            .tableName("customer")
            .operationType(WALEntry.OperationType.INSERT)
            .data(singleData)
            .build();
        
        // NEW WAY - Always use List, even for single entry
        System.out.println("   Calling: performCacheOpBatch(List.of(entry))");
        CacheOperationResponse response1 = cacheService.performCacheOpBatch(List.of(singleEntry));
        System.out.println("   Result: " + (response1.isSuccess() ? "SUCCESS" : "FAILED"));
        System.out.println();
        
        // Example 2: Multiple Entry Operation  
        System.out.println("2. MULTIPLE ENTRY OPERATION:");
        System.out.println("   Same API method - just pass multiple entries\n");
        
        Map<String, Object> data1 = new HashMap<>();
        data1.put("id", 100L);
        data1.put("balance", 1000.0);
        
        Map<String, Object> data2 = new HashMap<>();
        data2.put("id", 200L);
        data2.put("balance", 500.0);
        
        Map<String, Object> data3 = new HashMap<>();
        data3.put("id", 999L);
        data3.put("amount", 250.0);
        
        WALEntry entry1 = new WALEntry.Builder()
            .dbName("bankdb")
            .tableName("account")
            .operationType(WALEntry.OperationType.UPDATE)
            .data(data1)
            .build();
            
        WALEntry entry2 = new WALEntry.Builder()
            .dbName("bankdb")
            .tableName("account")
            .operationType(WALEntry.OperationType.UPDATE)
            .data(data2)
            .build();
            
        WALEntry entry3 = new WALEntry.Builder()
            .dbName("bankdb")
            .tableName("transaction")
            .operationType(WALEntry.OperationType.INSERT)
            .data(data3)
            .build();
        
        // Same method for batch - just pass a list
        System.out.println("   Calling: performCacheOpBatch(List.of(entry1, entry2, entry3))");
        CacheOperationResponse response2 = cacheService.performCacheOpBatch(
            List.of(entry1, entry2, entry3)
        );
        System.out.println("   Result: " + (response2.isSuccess() ? "SUCCESS" : "FAILED"));
        System.out.println();
        
        // Example 3: Building lists dynamically
        System.out.println("3. DYNAMIC LIST BUILDING:");
        System.out.println("   Easy to build entry lists programmatically\n");
        
        List<WALEntry> dynamicEntries = new ArrayList<>();
        
        // Add entries conditionally
        if (true) { // some condition
            dynamicEntries.add(singleEntry);
        }
        
        // Add more entries in a loop
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> loopData = new HashMap<>();
            loopData.put("id", (long) i);
            loopData.put("value", i * 100);
            
            dynamicEntries.add(new WALEntry.Builder()
                .dbName("testdb")
                .tableName("data")
                .operationType(WALEntry.OperationType.INSERT)
                .data(loopData)
                .build());
        }
        
        System.out.println("   Built list with " + dynamicEntries.size() + " entries");
        System.out.println("   Calling: performCacheOpBatch(dynamicEntries)");
        CacheOperationResponse response3 = cacheService.performCacheOpBatch(dynamicEntries);
        System.out.println("   Result: " + (response3.isSuccess() ? "SUCCESS" : "FAILED"));
        System.out.println();
        
        System.out.println("========================================");
        System.out.println("KEY BENEFITS OF SIMPLIFIED API:");
        System.out.println("========================================");
        System.out.println("✓ ONE method for everything - no confusion");
        System.out.println("✓ Always atomic - all operations are batches");
        System.out.println("✓ Simpler code - less branching logic");
        System.out.println("✓ Fewer errors - can't call wrong method");
        System.out.println("✓ Better performance - always uses batch path");
        System.out.println("✓ Cleaner interface - minimal API surface");
        System.out.println();
        
        System.out.println("MIGRATION GUIDE:");
        System.out.println("================");
        System.out.println("Old: performCacheOpSingle(entry)");
        System.out.println("New: performCacheOpBatch(List.of(entry))");
        System.out.println();
        System.out.println("Old: performCacheOpBatch(entries)");
        System.out.println("New: performCacheOpBatch(entries) - NO CHANGE!");
        System.out.println();
        
        System.out.println("That's it! Simply wrap single entries with List.of()");
    }
}