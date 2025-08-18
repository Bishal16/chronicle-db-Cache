# Final Cache Architecture with Initial Configuration + Lazy Loading

## Overview

The cache system now follows a two-phase loading strategy:
1. **Initial Load**: Registry constructor accepts a list of db-table pairs to load at startup
2. **Lazy Load**: Any db-table not in the initial list is loaded on first access

## How It Works

### 1. Initialization Flow

```java
// During startup
List<String> databases = discoverDatabases(); // ["res_1", "res_2", "admin"]

// Build initial configuration
List<CacheConfig> initialCaches = [
    new CacheConfig("res_1", "packageaccount"),
    new CacheConfig("res_1", "packageaccountreserve"),
    new CacheConfig("res_2", "packageaccount"),
    new CacheConfig("res_2", "packageaccountreserve"),
    new CacheConfig("admin", "packageaccount"),
    new CacheConfig("admin", "packageaccountreserve")
];

// Create registry with initial caches
CacheRegistry registry = new CacheRegistry(
    dataSource, 
    appender,
    initialCaches,    // These are loaded immediately
    cacheFactory      // Factory for creating cache instances
);
```

### 2. Registry After Initialization

```
Registry State After Init:
{
    "res_1-packageaccount"         -> DbCache (loaded, 1000 entities)
    "res_1-packageaccountreserve"  -> DbCache (loaded, 50 entities)
    "res_2-packageaccount"         -> DbCache (loaded, 2000 entities)
    "res_2-packageaccountreserve"  -> DbCache (loaded, 100 entities)
    "admin-packageaccount"         -> DbCache (loaded, 500 entities)
    "admin-packageaccountreserve"  -> DbCache (loaded, 25 entities)
}
```

### 3. Lazy Loading for Missing Entries

When accessing a database not in the initial list:

```java
// New database res_99 wasn't in initial configuration
PackageAccount account = cacheManager.getPackageAccount("res_99", 123);

// Triggers lazy loading:
// 1. Check registry: "res_99-packageaccount" not found
// 2. Discover primary key: "id_packageaccount"
// 3. Create cache instance using factory
// 4. Load all data from res_99.packageaccount
// 5. Register in registry as "res_99-packageaccount"
// 6. Return entity 123
```

## Implementation Details

### CacheRegistry Constructor

```java
public CacheRegistry(DataSource dataSource, 
                    ExcerptAppender appender,
                    List<CacheConfig> initialCaches, 
                    CacheFactory cacheFactory) {
    this.dataSource = dataSource;
    this.walWriter = new WALWriter(appender);
    this.defaultCacheFactory = cacheFactory;
    
    // Initialize specified caches
    if (initialCaches != null && !initialCaches.isEmpty()) {
        initializeConfiguredCaches(initialCaches);
    }
}
```

### CacheConfig Class

```java
public static class CacheConfig {
    public final String dbName;
    public final String tableName;
    public final boolean loadOnInit;  // Whether to load data immediately
    
    public CacheConfig(String dbName, String tableName) {
        this(dbName, tableName, true);
    }
}
```

### PackageAccountCache Initialization

```java
void initCacheAfterReplay() {
    // Discover all known databases
    List<String> databases = discoverDatabases();
    
    // Build initial cache list
    List<CacheConfig> initialCaches = new ArrayList<>();
    for (String dbName : databases) {
        initialCaches.add(new CacheConfig(dbName, "packageaccount"));
        initialCaches.add(new CacheConfig(dbName, "packageaccountreserve"));
    }
    
    // Create factory
    CacheFactory factory = this::createCacheInstance;
    
    // Initialize registry with initial caches
    cacheRegistry = new CacheRegistry(dataSource, appender, initialCaches, factory);
    
    // Set factory for lazy loading
    cacheRegistry.setDefaultCacheFactory(factory);
}
```

## Benefits of This Approach

### 1. **Predictable Startup**
- Known databases are loaded at startup
- Startup time is predictable
- Critical caches are ready immediately

### 2. **Memory Efficiency**
- Only load databases that are actually used
- New/rarely used databases don't consume memory until accessed

### 3. **Dynamic Flexibility**
- New databases work automatically without restart
- Temporary databases can be cached on-demand
- No configuration changes needed for new databases

### 4. **Performance Optimization**
- Initial load happens during startup (when load is low)
- Lazy loading spreads load over time
- First access to new database has one-time cost

## Usage Scenarios

### Scenario 1: Normal Operation
```java
// Database "res_1" was in initial configuration
PackageAccount acc = cacheManager.getPackageAccount("res_1", 123);
// Returns immediately from pre-loaded cache
```

### Scenario 2: New Database Added
```java
// Database "res_99" added after system started
PackageAccount acc = cacheManager.getPackageAccount("res_99", 456);
// Triggers lazy loading on first access
// Subsequent accesses use cached data
```

### Scenario 3: Checking Cache Status
```java
// Get registry statistics
RegistryStats stats = cacheRegistry.getStats();
// Shows: 6 initial caches + any lazy-loaded caches

// Check specific cache
boolean exists = cacheRegistry.hasCache("res_99", "packageaccount");

// Get all cache keys
Set<String> keys = cacheRegistry.getCacheKeys();
// ["res_1-packageaccount", ..., "res_99-packageaccount"]
```

## Log Output Example

```
INFO: Discovered 3 databases: [res_1, res_2, admin]
INFO: Configuring registry with 6 initial caches for 3 databases
INFO: Initializing 6 configured caches...
INFO: Initializing cache for res_1-packageaccount
INFO: ✅ Initialized cache for res_1-packageaccount
INFO: Initializing cache for res_1-packageaccountreserve
INFO: ✅ Initialized cache for res_1-packageaccountreserve
...
INFO: Cache initialization complete: 6 successful, 0 failed
INFO: ✅ Cache registry initialized: RegistryStats{caches=6, databases=3, tables=2, entities=3675}

// Later, when res_99 is accessed:
INFO: Cache not found for res_99-packageaccount, lazy loading from database...
INFO: ✅ Successfully lazy-loaded cache for res_99-packageaccount
```

## Architecture Summary

```
                    Application Layer
                           |
                    PackageAccountCache
                           |
                    CacheRegistry
                    /            \
         Initial Load            Lazy Load
         (at startup)           (on demand)
              |                      |
     res_1, res_2, admin         res_99, res_100...
              |                      |
         Pre-loaded              Loaded when
         and ready               first accessed
```

## Configuration Pattern

This architecture provides the perfect balance:
- **Predictable**: Known databases loaded at startup
- **Flexible**: Unknown databases loaded on-demand
- **Efficient**: Only load what's actually used
- **Simple**: Single registry manages everything

The system is now:
✅ Initialized with known db-table pairs  
✅ Capable of lazy loading unknown entries  
✅ Memory efficient  
✅ Thread-safe  
✅ Production ready