# WALEntryBatch System - Step-by-Step Testing Guide

## 🎯 **Overview**
This guide provides step-by-step instructions to test your WALEntryBatch system comprehensively.

## 📋 **Prerequisites**

### **1. Verify Java Version**
```bash
java --version
# Should show Java 11 or higher
```

### **2. Ensure Project is Compiled**
```bash
mvn clean compile -q
# Should complete without errors
```

### **3. Check Chronicle Queue Module Access** (Optional - for server tests)
```bash
# Add these JVM args if needed for Chronicle Queue:
--add-exports java.base/java.lang.reflect=ALL-UNNAMED 
--add-opens java.base/java.lang.reflect=ALL-UNNAMED 
--add-opens java.base/java.nio=ALL-UNNAMED 
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

---

## 🧪 **TEST LEVEL 1: Basic WALEntryBatch Creation (No Server Required)**

### **Step 1.1: Run SimpleWALBatchTest**
```bash
# Method 1: Using Maven
mvn exec:java -Dexec.mainClass="com.telcobright.oltp.example.SimpleWALBatchTest"

# Method 2: Direct Java (if Method 1 doesn't show output)
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
java -cp "target/classes:$(cat cp.txt)" com.telcobright.oltp.example.SimpleWALBatchTest
```

### **Expected Results:**
```
╔══════════════════════════════════════════════════════════════╗
║              Simple WALBatch Test - Demonstration            ║
║               (No server required - shows concept)           ║
╚══════════════════════════════════════════════════════════════╝

=== WALEntryBatch Creation Demo ===
1. Single Database Batch:
   Transaction ID: DEMO_SINGLE_[uuid]
   Entries: 2
   Databases: [telcobright]

2. Multi-Database Batch (OP1 across 3 databases):
   Transaction ID: DEMO_MULTI_[uuid]
   Entries: 6
   Databases: [telcobright, res_1, res_2]
```

### **✅ Verification Checklist:**
- [ ] Test runs without errors
- [ ] Shows single database batch with 2 entries
- [ ] Shows multi-database batch with 6 entries across 3 databases
- [ ] Each batch has unique transaction ID
- [ ] No duplicate transaction IDs per entry

---

## 🧪 **TEST LEVEL 2: Builder Pattern Verification**

### **Step 2.1: Run SimpleBuilderTest**
```bash
mvn exec:java -Dexec.mainClass="com.telcobright.oltp.example.SimpleBuilderTest"
```

### **Step 2.2: Test Custom Builder**
Create a quick test file:
```bash
cat > QuickBuilderTest.java << 'EOF'
import com.telcobright.oltp.grpc.builder.WALBatchGrpcClient;
import com.telcobright.core.wal.WALEntryBatch;
import java.util.UUID;

public class QuickBuilderTest {
    public static void main(String[] args) {
        System.out.println("=== Builder Pattern Test ===");
        
        // Test your original OP1 pattern
        WALEntryBatch batch = WALBatchGrpcClient.batchBuilder()
            .transactionId("TEST_" + UUID.randomUUID().toString().substring(0, 8))
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
        
        System.out.println("✅ Builder Pattern Success!");
        System.out.println("Transaction ID: " + batch.getTransactionId());
        System.out.println("Entries: " + batch.size());
        System.out.println("Databases: " + batch.getDatabaseNames());
        
        // Test individual operations
        System.out.println("\n=== Individual Entries ===");
        for (int i = 0; i < batch.size(); i++) {
            var entry = batch.get(i);
            System.out.println("Entry[" + i + "]: " + entry.getOperationType() + 
                " on " + entry.getDbName() + "." + entry.getTableName());
        }
    }
}
EOF

# Compile and run
javac -cp "target/classes:$(cat cp.txt)" QuickBuilderTest.java
java -cp ".:target/classes:$(cat cp.txt)" QuickBuilderTest
```

### **✅ Verification Checklist:**
- [ ] Builder creates WALEntryBatch successfully
- [ ] Fluent API works (chained method calls)
- [ ] Multi-database entries created correctly
- [ ] All operation types work (UPDATE, INSERT, DELETE)

---

## 🧪 **TEST LEVEL 3: gRPC Protocol Testing (Server Required)**

### **Step 3.1: Start Test Server**
```bash
# Terminal 1: Start the WAL batch test server
java --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens java.base/java.nio=ALL-UNNAMED \
     -cp "target/classes:$(cat cp.txt)" \
     com.telcobright.oltp.example.WALBatchTestServer
```

**Expected Server Output:**
```
🚀 WALBatch gRPC Test Server starting...
📡 Server listening on port: 9000
✅ WALBatchGrpcService registered
⏳ Server ready for WALBatch requests...
```

### **Step 3.2: Run Test Client**
```bash
# Terminal 2: Run the test client
java --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "target/classes:$(cat cp.txt)" \
     com.telcobright.oltp.example.WALBatchTestClient
```

**Expected Client Output:**
```
╔══════════════════════════════════════════════════════════════╗
║                    WAL Batch gRPC Client Test               ║
╚══════════════════════════════════════════════════════════════╝

🔄 Running Test Case 1: Single Database OP1...
✅ Test Case 1 SUCCESS: entries_processed=2

🔄 Running Test Case 4: Multi-Database OP1...
✅ Test Case 4 SUCCESS: entries_processed=6, databases=3
```

### **✅ Verification Checklist:**
- [ ] Server starts without errors
- [ ] Client connects successfully
- [ ] All 4 test cases pass
- [ ] Multi-database test processes 6 entries
- [ ] Server logs show WALBatch received correctly

---

## 🧪 **TEST LEVEL 4: Full System Integration**

### **Step 4.1: Start Quarkus Application**
```bash
# Start full application with WAL batch support
mvn quarkus:dev
```

### **Step 4.2: Test with Various Clients**

#### **Test Case 1: Basic Operations**
```bash
# Run basic test cases
java -cp "target/classes:$(cat cp.txt)" com.telcobright.oltp.example.TestCasesGrpcClient
```

#### **Test Case 2: Payload Testing**
```bash
# Test payload transmission
java -cp "target/classes:$(cat cp.txt)" com.telcobright.oltp.example.PayloadTestClient
```

#### **Test Case 3: Batch CRUD Operations**
```bash
# Test batch CRUD
java -cp "target/classes:$(cat cp.txt)" com.telcobright.oltp.example.BatchCrudGrpcClient
```

---

## 🔍 **Troubleshooting Common Issues**

### **Issue 1: No Console Output**
```bash
# Solution: Use explicit classpath and JVM args
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
java --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "target/classes:$(cat cp.txt)" \
     [ClassName] 2>&1 | tee output.log
```

### **Issue 2: Chronicle Queue Module Errors**
```bash
# Add these JVM arguments:
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
--add-exports java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

### **Issue 3: Server Won't Start**
Check if Chronicle Queue startup is disabled:
```bash
# Look for @Startup annotations commented out in:
# src/main/java/com/telcobright/oltp/service/ConsumerFactory.java
# src/main/java/com/telcobright/oltp/service/ChronicleInstanceFactory.java
```

### **Issue 4: gRPC Connection Refused**
```bash
# Check if server is running on correct port
netstat -ln | grep 9000
# Or try different port
java ... -Dgrpc.server.port=9001 ...
```

---

## 📊 **Success Criteria for Each Test Level**

### **Level 1 Success:**
- ✅ WALEntryBatch objects created
- ✅ Transaction ID at batch level only
- ✅ Multi-database batches work

### **Level 2 Success:**
- ✅ Builder pattern API functional
- ✅ All operation types supported
- ✅ Fluent interface works

### **Level 3 Success:**
- ✅ gRPC server starts
- ✅ Client-server communication works
- ✅ Proto conversion successful
- ✅ All test cases pass

### **Level 4 Success:**
- ✅ Full Quarkus integration
- ✅ Chronicle Queue integration
- ✅ Database operations (simulated)
- ✅ End-to-end atomic transactions

---

## 🎯 **Quick Verification Commands**

```bash
# 1. Quick compilation check
mvn compile -q && echo "✅ Compilation OK" || echo "❌ Compilation Failed"

# 2. Quick WALBatch test
mvn exec:java -Dexec.mainClass="com.telcobright.oltp.example.SimpleWALBatchTest" -q

# 3. Quick builder test  
echo 'import com.telcobright.oltp.grpc.builder.WALBatchGrpcClient; public class Q { public static void main(String[] a) { System.out.println("Builder: " + WALBatchGrpcClient.batchBuilder().transactionId("TEST").build().getTransactionId()); }}' > Q.java && javac -cp "target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" Q.java && java -cp ".:target/classes:$(mvn dependency:build-classpath -q -Dmdep.outputFile=/dev/stdout)" Q

# 4. Clean up
rm -f Q.java Q.class cp.txt QuickBuilderTest.java QuickBuilderTest.class
```

---

## 🚀 **Your Original Request Test**

**The most important test** - your multi-database OP1 pattern:
```java
WALEntryBatch batch = WALBatchGrpcClient.batchBuilder()
    .transactionId("MULTI_DB_TXN")
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
    
// Result: 6 entries, 3 databases, 1 transaction ID ✅
```

This is tested in **SimpleWALBatchTest.java** and confirmed working! 🎉