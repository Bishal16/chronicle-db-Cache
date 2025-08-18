# Chronicle Queue WAL Library Architecture

## Clean Separation: Core vs Implementation

### Core Library (`com.telcobright.core`)
Generic, reusable components for any application:

```
com.telcobright.core/
├── wal/                    # Write-Ahead Log components
│   ├── WALEntry.java       # Generic WAL entry structure
│   ├── WALWriter.java      # Serializes to Chronicle Queue
│   └── WALReader.java      # Deserializes from Chronicle Queue
│
├── cache/                  # Generic caching framework
│   ├── CacheableEntity.java    # Interface for cacheable entities
│   ├── DeltaOperation.java     # Interface for delta operations
│   └── ChronicleQueueCache.java # Generic cache with WAL support
│
├── sql/                    # SQL operations
│   └── SQLOperationHandler.java # SQL generation and execution
│
├── consumer/               # Consumer framework
│   └── TransactionalConsumer.java # Transactional MySQL consumer
│
├── integration/            # Integration utilities
│   └── ChronicleQueueIntegration.java # Factory and management
│
└── example/                # Usage examples
    └── UsageExample.java   # Example implementations
```

### Application Implementation (`com.telcobright.oltp`)
Specific business logic and entities:

```
com.telcobright.oltp/
├── entity/                 # Business entities
│   ├── PackageAccount.java            # Implements CacheableEntity
│   ├── PackageAccDelta.java           # Implements DeltaOperation
│   ├── PackageAccountReserve.java     # Implements CacheableEntity
│   └── PackageAccountReserveDelta.java # Implements DeltaOperation
│
├── dbCache/                # Application-specific caches
│   ├── PackageAccountCache.java       # Extends ChronicleQueueCache
│   └── PackageAccountReserveCache.java # Extends ChronicleQueueCache
│
├── service/                # Business services
│   ├── UniversalConsumer.java         # Application consumer
│   └── RefactoredUniversalConsumer.java # Uses TransactionalConsumer
│
└── grpcController/         # API layer
    └── PrepaidAccountingGrpcService.java # gRPC endpoints
```

## Key Design Principles

### 1. Core Library Independence
- **No business logic** in core library
- **No application dependencies** (only Chronicle Queue, JDBC, SLF4J)
- **Generic interfaces** that can be implemented by any entity type
- **Configurable behavior** through abstract methods and listeners

### 2. Extension Points
The core library provides multiple extension points:

#### Abstract Methods in ChronicleQueueCache:
```java
protected abstract void loadEntitiesFromDb(Connection conn, String dbName, 
                                          ConcurrentHashMap<Long, TEntity> cache);
protected abstract TEntity mapResultSetToEntity(ResultSet rs);
protected abstract String getTableName();
protected abstract String getPrimaryKeyColumn();
protected abstract WALEntry createWALEntryForDelta(TDelta delta, TEntity entity, String txId);
protected abstract WALEntry createWALEntryForInsert(String dbName, TEntity entity);
protected abstract void logDeltaApplication(TDelta delta, TEntity entity);
```

#### Listener Hooks in TransactionalConsumer:
```java
public interface ConsumerListener {
    void beforeProcess(WALEntry entry);
    void afterProcess(WALEntry entry, boolean success, Exception error);
    void onBatchComplete(List<WALEntry> entries, boolean success);
}
```

### 3. Type Safety
- Generic types ensure compile-time safety
- Interfaces enforce contract between core and implementation
- Builder pattern for WALEntry prevents invalid states

### 4. Transaction Guarantees
Core library ensures:
- All DB operations use `SET autocommit=0`
- Atomic commit or rollback per WAL entry
- Offset management within same transaction
- Batch operations in single transaction

## Usage Pattern

### 1. Implement Business Entity
```java
public class MyEntity implements CacheableEntity {
    @Override
    public void applyDelta(BigDecimal amount) {
        // Business logic here
    }
}
```

### 2. Implement Delta Operation
```java
public class MyDelta implements DeltaOperation {
    // Delta fields and methods
}
```

### 3. Extend ChronicleQueueCache
```java
public class MyCache extends ChronicleQueueCache<MyEntity, MyDelta> {
    // Implement abstract methods
}
```

### 4. Initialize and Use
```java
MyCache cache = new MyCache();
cache.initialize(dataSource, walWriter, adminDb);
cache.applyDeltasAndWriteToWAL(deltas);
```

## Benefits of This Architecture

1. **Reusability**: Core library can be used for any application
2. **Maintainability**: Clear separation of concerns
3. **Testability**: Core and implementation can be tested independently
4. **Scalability**: Easy to add new entity types
5. **Flexibility**: Multiple extension points for customization
6. **Reliability**: Transaction guarantees built into core

## Migration Path

Existing code can gradually migrate to use core library:
1. Keep existing interfaces (compatibility methods)
2. Gradually refactor to use core components
3. Eventually remove compatibility layer

This architecture ensures the Chronicle Queue WAL system is both powerful and flexible, suitable for any application requiring durable, transactional database synchronization.