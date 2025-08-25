package com.telcobright.core.example;

import com.telcobright.core.consumer.TransactionalConsumer;
import com.telcobright.core.integration.ChronicleQueueIntegration;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.core.wal.WALWriter;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

/**
 * Example usage of the generic Chronicle Queue WAL library.
 * This demonstrates how to use the library for any application.
 */
public class UsageExample {
    
    public static void main(String[] args) throws Exception {
        // 1. Configure DataSource
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:mysql://103.95.96.77:3306/telcobright");
        hikariConfig.setUsername("tbuser");
        hikariConfig.setPassword("Takay1takaane$");
        hikariConfig.setMaximumPoolSize(10);
        
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        
        // 2. Configure Chronicle Queue Integration
        ChronicleQueueIntegration.Config config = new ChronicleQueueIntegration.Config()
            .withQueuePath("ex-wal-queue")
            .withOffsetDb("admin")
            .withOffsetTable("consumer_offsets")
            .withConsumerCount(2)
            .withBlockSize(128 * 1024 * 1024); // 128MB blocks
        
        // 3. Create the integration
        ChronicleQueueIntegration integration = new ChronicleQueueIntegration(dataSource, config);
        
        // 4. Create a WAL writer for producers
        WALWriter walWriter = integration.createWALWriter();
        
        // 5. Create consumers with custom listeners
        TransactionalConsumer consumer1 = integration.createConsumer(
            "consumer-1",
            new TransactionalConsumer.ConsumerListener() {
                @Override
                public void beforeProcess(WALEntry entry) {
                    System.out.println("Consumer 1 processing: " + entry.getFullTableName());
                }
                
                @Override
                public void afterProcess(WALEntry entry, boolean success, Exception error) {
                    if (success) {
                        System.out.println("Consumer 1 completed: " + entry.getFullTableName());
                    } else {
                        System.err.println("Consumer 1 failed: " + error.getMessage());
                    }
                }
                
                @Override
                public void onBatchComplete(java.util.List<WALEntry> entries, boolean success) {
                    System.out.println("Consumer 1 batch complete: " + entries.size() + " entries");
                }
            }
        );
        
        TransactionalConsumer consumer2 = integration.createConsumer("consumer-2");
        
        // 6. Start consumers
        integration.startConsumers();
        
        // 7. Example: Write some entries to WAL
        
        // Example INSERT
        WALEntry insertEntry = new WALEntry.Builder("mydb", "users", WALEntry.OperationType.INSERT)
            .withData("id", 123L)
            .withData("name", "John Doe")
            .withData("email", "john@example.com")
            .withData("balance", new BigDecimal("100.50"))
            .build();
        
        walWriter.write(insertEntry);
        System.out.println("✅ Wrote INSERT entry to WAL");
        
        // Example UPDATE
        WALEntry updateEntry = new WALEntry.Builder("mydb", "users", WALEntry.OperationType.UPDATE)
            .withData("balance", new BigDecimal("150.75"))
            .withData("where_id", 123L)  // WHERE conditions use "where_" prefix
            .build();
        
        walWriter.write(updateEntry);
        System.out.println("✅ Wrote UPDATE entry to WAL");
        
        // Example DELETE
        WALEntry deleteEntry = new WALEntry.Builder("mydb", "users", WALEntry.OperationType.DELETE)
            .withData("where_id", 456L)
            .build();
        
        walWriter.write(deleteEntry);
        System.out.println("✅ Wrote DELETE entry to WAL");
        
        // Example UPSERT
        WALEntry upsertEntry = new WALEntry.Builder("mydb", "products", WALEntry.OperationType.UPSERT)
            .withData("id", 789L)
            .withData("name", "Product X")
            .withData("price", new BigDecimal("29.99"))
            .withData("stock", 100)
            .build();
        
        walWriter.write(upsertEntry);
        System.out.println("✅ Wrote UPSERT entry to WAL");
        
        // Example BATCH transaction
        String txId = UUID.randomUUID().toString();
        WALEntry[] batchEntries = new WALEntry[] {
            new WALEntry.Builder("mydb", "orders", WALEntry.OperationType.INSERT)
                .withData("id", 1001L)
                .withData("customer_id", 123L)
                .withData("total", new BigDecimal("299.99"))
                .build(),
            
            new WALEntry.Builder("mydb", "order_items", WALEntry.OperationType.INSERT)
                .withData("order_id", 1001L)
                .withData("product_id", 789L)
                .withData("quantity", 10)
                .withData("price", new BigDecimal("29.99"))
                .build(),
            
            new WALEntry.Builder("mydb", "products", WALEntry.OperationType.UPDATE)
                .withData("stock", 90)  // Reduce stock
                .withData("where_id", 789L)
                .build()
        };
        
        // Set transaction ID for all entries
        for (WALEntry entry : batchEntries) {
            entry.setTransactionId(txId);
        }
        walWriter.write(java.util.Arrays.asList(batchEntries));
        System.out.println("✅ Wrote BATCH transaction to WAL with txId: " + txId);
        
        // 8. Monitor queue statistics
        Thread.sleep(2000); // Let consumers process
        
        ChronicleQueueIntegration.QueueStats stats = integration.getStats();
        System.out.println("Queue Statistics: " + stats);
        
        // 9. Graceful shutdown
        System.out.println("\nPress Enter to shutdown...");
        System.in.read();
        
        integration.shutdown();
        dataSource.close();
        
        System.out.println("✅ Application shutdown complete");
    }
    
    /**
     * Example of custom application-specific usage
     */
    public static class ApplicationSpecificExample {
        
        private final WALWriter walWriter;
        private final String dbName;
        
        public ApplicationSpecificExample(WALWriter walWriter, String dbName) {
            this.walWriter = walWriter;
            this.dbName = dbName;
        }
        
        /**
         * Example: Log a financial transaction
         */
        public void logTransaction(Long accountId, BigDecimal amount, String type) {
            WALEntry entry = new WALEntry.Builder(dbName, "transactions", WALEntry.OperationType.INSERT)
                .withData("account_id", accountId)
                .withData("amount", amount)
                .withData("transaction_type", type)
                .withData("timestamp", System.currentTimeMillis())
                .build();
            
            walWriter.write(entry);
        }
        
        /**
         * Example: Update account balance
         */
        public void updateBalance(Long accountId, BigDecimal newBalance) {
            WALEntry entry = new WALEntry.Builder(dbName, "accounts", WALEntry.OperationType.UPDATE)
                .withData("balance", newBalance)
                .withData("last_updated", System.currentTimeMillis())
                .withData("where_id", accountId)
                .build();
            
            walWriter.write(entry);
        }
        
        /**
         * Example: Batch operation for transfer
         */
        public void transfer(Long fromAccountId, Long toAccountId, BigDecimal amount) {
            String txId = UUID.randomUUID().toString();
            
            List<WALEntry> entries = List.of(
                // Debit from source account
                new WALEntry.Builder(dbName, "accounts", WALEntry.OperationType.UPDATE)
                    .withData("balance", "balance - " + amount) // SQL expression
                    .withData("where_id", fromAccountId)
                    .build(),
                
                // Credit to destination account
                new WALEntry.Builder(dbName, "accounts", WALEntry.OperationType.UPDATE)
                    .withData("balance", "balance + " + amount) // SQL expression
                    .withData("where_id", toAccountId)
                    .build(),
                
                // Log the transfer
                new WALEntry.Builder(dbName, "transfers", WALEntry.OperationType.INSERT)
                    .withData("from_account_id", fromAccountId)
                    .withData("to_account_id", toAccountId)
                    .withData("amount", amount)
                    .withData("transfer_date", System.currentTimeMillis())
                    .build()
            );
            
            // Set transaction ID for all entries
            for (WALEntry entry : entries) {
                entry.setTransactionId(txId);
            }
            
            walWriter.write(entries);
        }
    }
}