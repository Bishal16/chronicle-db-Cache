# Lazy Loading & Auto-Population Architecture

## Overview
The cache system now supports **automatic lazy loading** - if a cache for a specific database-table combination doesn't exist in the registry, it will automatically:
1. Create the cache instance
2. Discover the table's primary key
3. Load all data from the database
4. Register the cache in the registry

## How It Works

### 1. Request Flow with Lazy Loading

```
User Request: getPackageAccount("res_99", 123)
                    ↓
    PackageAccountCache.getPackageAccount()
                    ↓
    getOrCreatePackageAccountCache("res_99")
                    ↓
    cacheRegistry.getCache("res_99", "packageaccount")
                    ↓
            [Cache exists?]
            /            \
          YES            NO
           ↓              ↓
      Return cache   Create & populate
           ↓              ↓
      cache.get(123)  Load from DB
                          ↓
                    Register in registry
                          ↓
                    cache.get(123)
```

### 2. Core Implementation

#### CacheRegistry.getOrCreateCache()
```java
public <T> DbCache<T> getOrCreateCache(String dbName, String tableName, 
                                      CacheFactory cacheFactory) {
    String key = dbName + "-" + tableName;
    
    // Try to get existing cache
    DbCache<T> cache = (DbCache<T>) caches.get(key);
    if (cache != null) {
        return cache;  // Cache exists, return it
    }
    
    // Cache doesn't exist, create it
    logger.info("Cache not found for {}-{}, creating and populating...", dbName, tableName);
    
    // 1. Discover primary key
    String primaryKey = discoverPrimaryKey(dbName, tableName);
    
    // 2. Create cache using factory
    cache = cacheFactory.createCache(dbName, tableName, primaryKey);
    
    // 3. Register and load data from database
    registerAndInitialize(cache, true);  // true = load data
    
    return cache;
}
```

#### PackageAccountCache Helper Methods
```java
private DbCache<PackageAccount> getOrCreatePackageAccountCache(String dbName) {
    // First try to get existing cache
    DbCache<PackageAccount> cache = cacheRegistry.getCache(dbName, "packageaccount");
    if (cache != null) {
        return cache;
    }
    
    // Cache doesn't exist, create it with lazy loading
    return cacheRegistry.getOrCreateCache(dbName, "packageaccount", 
        (db, table, primaryKey) -> new DbCache<PackageAccount>(
            db, table, primaryKey,
            this::mapPackageAccount,      // Mapper function
            this::applyPackageAccountDelta // Delta applier function
        )
    );
}
```

### 3. Primary Key Discovery

The system automatically discovers the primary key column:

```java
private String discoverPrimaryKey(String dbName, String tableName) {
    String sql = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE CONSTRAINT_NAME = 'PRIMARY' 
        AND TABLE_SCHEMA = ? 
        AND TABLE_NAME = ?
        LIMIT 1
    """;
    // Execute and return primary key column name
}
```

## Usage Scenarios

### Scenario 1: Normal Operation (Cache Exists)
```java
// Cache already loaded for res_1-packageaccount
PackageAccount account = cacheManager.getPackageAccount("res_1", 123);
// Returns immediately from cache
```

### Scenario 2: First Access to New Database
```java
// First time accessing res_99 database (not in initial load)
PackageAccount account = cacheManager.getPackageAccount("res_99", 456);
// Triggers:
// 1. Create cache for res_99-packageaccount
// 2. Load all records from res_99.packageaccount table
// 3. Register cache in registry
// 4. Return account 456
```

### Scenario 3: Dynamic Database Addition
```java
// New reseller database created at runtime
String newDb = "res_" + System.currentTimeMillis();
createDatabase(newDb);
createTables(newDb);

// First access automatically creates and populates cache
PackageAccount account = cacheManager.getPackageAccount(newDb, 1);
// Cache automatically created and populated!
```

## Benefits

### 1. **No Manual Cache Registration**
- Don't need to pre-register all database caches
- New databases automatically supported

### 2. **Memory Efficiency**
- Only load caches that are actually used
- Unused database caches don't consume memory

### 3. **Dynamic Scaling**
- Add new databases without restarting
- Remove databases without code changes

### 4. **Fault Tolerance**
- If a database is temporarily unavailable during startup, it can still be cached later
- Partial initialization doesn't break the system

### 5. **Simplified Configuration**
- No need to maintain a list of all databases
- System discovers and caches on demand

## Performance Considerations

### First Access Cost
- First access to a new database-table triggers full table load
- This is a one-time cost per database-table combination
- Subsequent accesses are served from cache

### Optimization Strategies

#### 1. Pre-warming Important Caches
```java
// During initialization, pre-load critical databases
List<String> criticalDatabases = Arrays.asList("admin", "res_1", "res_2");
for (String db : criticalDatabases) {
    getOrCreatePackageAccountCache(db);  // Pre-warm cache
}
```

#### 2. Background Loading
```java
// Load non-critical caches in background
CompletableFuture.runAsync(() -> {
    for (String db : nonCriticalDatabases) {
        getOrCreatePackageAccountCache(db);
    }
});
```

#### 3. Partial Loading
```java
// For very large tables, consider partial loading
DbCache<T> cache = new DbCache<>(dbName, tableName, primaryKey, 
    mapper, deltaApplier);
cache.loadFromDatabase("WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)");
```

## Monitoring & Debugging

### Check Cache Status
```java
// Check if cache exists
boolean exists = cacheRegistry.hasCache("res_99", "packageaccount");

// Get all cached databases
Set<String> keys = cacheRegistry.getCacheKeys();
// ["res_1-packageaccount", "res_2-packageaccount", ...]

// Get statistics
CacheRegistry.RegistryStats stats = cacheRegistry.getStats();
System.out.println("Total caches: " + stats.totalCaches);
System.out.println("Total entities: " + stats.totalCachedEntities);
```

### Log Output Examples
```
INFO: Cache not found for res_99-packageaccount, creating and populating from database...
INFO: Discovered primary key 'id_packageaccount' for res_99.packageaccount
INFO: Loaded 5432 entities from res_99.packageaccount
INFO: ✅ Successfully created and populated cache for res_99-packageaccount
```

## Thread Safety

The lazy loading mechanism is thread-safe:
- `ConcurrentHashMap` ensures only one cache instance per key
- Database loading is synchronized per cache
- Multiple threads can safely request the same cache

## Error Handling

### Database Unavailable
```java
DbCache<T> cache = getOrCreatePackageAccountCache("res_invalid");
// Returns null if database doesn't exist or is unavailable
// Application continues to work for other databases
```

### Table Not Found
```java
DbCache<T> cache = getOrCreateCache("res_1", "non_existent_table");
// Returns null, logs error
// No crash, other caches continue to work
```

## Summary

The lazy loading system provides:
- ✅ **Automatic cache creation** on first access
- ✅ **Dynamic database support** without configuration
- ✅ **Memory efficiency** through on-demand loading
- ✅ **Fault tolerance** for partial failures
- ✅ **Thread-safe** concurrent access
- ✅ **Zero configuration** for new databases

This makes the system truly dynamic and self-managing!