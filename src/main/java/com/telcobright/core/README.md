# Chronicle Queue Generic WAL Library

A reusable, generic Write-Ahead Logging (WAL) library built on Chronicle Queue for transactional database synchronization.

## Overview

This library provides a complete solution for:
- Writing database operations to a persistent WAL using Chronicle Queue
- Consuming WAL entries and applying them to MySQL transactionally
- Managing consumer offsets for replay capability
- Supporting multi-database/multi-tenant architectures

## Core Components

### 1. WAL Entry (`com.telcobright.core.wal.WALEntry`)
Generic data structure representing any database operation with:
- Database and table names
- Operation type (INSERT, UPDATE, DELETE, UPSERT, BATCH)
- Flexible data map for column values
- Transaction ID support for batch operations

### 2. WAL Writer (`com.telcobright.core.wal.WALWriter`)
Serializes WAL entries to Chronicle Queue:
- Single entry writes
- Batch transaction writes
- Automatic indexing and logging

### 3. WAL Reader (`com.telcobright.core.wal.WALReader`)
Deserializes Chronicle Queue entries back to WAL entries:
- Type-safe value reading
- Batch entry support

### 4. SQL Operation Handler (`com.telcobright.core.sql.SQLOperationHandler`)
Generates and executes SQL from WAL entries:
- Supports all CRUD operations
- Handles WHERE conditions (prefix with "where_")
- Type-safe parameter binding

### 5. Transactional Consumer (`com.telcobright.core.consumer.TransactionalConsumer`)
Processes WAL entries with full transaction support:
- MySQL transactions with autocommit=0
- Commit on success, rollback on failure
- Consumer offset management
- Listener hooks for custom processing

### 6. Integration Factory (`com.telcobright.core.integration.ChronicleQueueIntegration`)
Simplifies setup and management:
- Queue configuration
- Consumer lifecycle management
- Statistics and monitoring

## Usage Example

```java
// 1. Setup DataSource
HikariDataSource dataSource = createDataSource();

// 2. Configure and create integration
ChronicleQueueIntegration.Config config = new ChronicleQueueIntegration.Config()
    .withQueuePath("./my-wal-queue")
    .withOffsetDb("admin")
    .withOffsetTable("consumer_offsets");

ChronicleQueueIntegration integration = new ChronicleQueueIntegration(dataSource, config);

// 3. Create WAL writer for producers
WALWriter walWriter = integration.createWALWriter();

// 4. Write operations to WAL
WALEntry entry = new WALEntry.Builder("mydb", "users", WALEntry.OperationType.INSERT)
    .withData("id", 123L)
    .withData("name", "John Doe")
    .withData("balance", new BigDecimal("100.00"))
    .build();

walWriter.write(entry);

// 5. Create and start consumers
TransactionalConsumer consumer = integration.createConsumer("consumer-1");
integration.startConsumers();
```

## Transaction Support

The library ensures full ACID compliance:

```sql
-- Each WAL entry is processed as:
SET autocommit=0;
-- Execute SQL operation
-- Update consumer offset
COMMIT;  -- or ROLLBACK on error
```

## Batch Operations

Support for atomic batch transactions:

```java
String txId = UUID.randomUUID().toString();
WALEntry[] entries = new WALEntry[] {
    // Multiple related operations
};
walWriter.writeBatch(entries, txId);
```

## Custom Processing

Extend behavior with listeners:

```java
consumer.setListener(new TransactionalConsumer.ConsumerListener() {
    @Override
    public void beforeProcess(WALEntry entry) {
        // Update cache before DB
    }
    
    @Override
    public void afterProcess(WALEntry entry, boolean success, Exception error) {
        // Log or handle result
    }
});
```

## Multi-Tenant Support

The library natively supports multi-database operations:
- Each WAL entry contains database name
- Consumers apply operations to correct database
- Offset tracking per consumer across all databases

## Architecture Benefits

1. **Durability**: All operations written to persistent WAL before DB
2. **Replay**: Resume from any point using consumer offsets
3. **Performance**: Async processing with Chronicle Queue
4. **Consistency**: Transactional processing ensures data integrity
5. **Scalability**: Multiple consumers can process in parallel

## Dependencies

- Chronicle Queue
- HikariCP (or any DataSource)
- MySQL Connector
- SLF4J for logging

## Thread Safety

All components are thread-safe:
- WALWriter can be shared across threads
- Multiple consumers can run concurrently
- Cache operations are synchronized

## Monitoring

Get queue statistics:

```java
ChronicleQueueIntegration.QueueStats stats = integration.getStats();
// Returns: queue path, consumer count, active consumers, last index
```

## Graceful Shutdown

```java
integration.shutdown(); // Stops consumers, closes queue
dataSource.close();
```