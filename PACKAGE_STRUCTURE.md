# Package Structure

The codebase is organized into three main packages:

## 1. `com.telcobright.api` - Public API
**The ONLY public interface exposed to external clients**

- `CacheServiceProxy` - Public interface with just 2 methods:
  - `performCacheOpBatch(WALEntryBatch)`
  - `performCacheOpSingle(WALEntry)`
- `CacheOperationResponse` - Simple response object
- `CacheServiceProxyImpl` - Proxy implementation

## 2. `com.telcobright.core` - Core Implementation
**All internal implementation - NOT directly accessible to external clients**

### `core.adapter` - Transport Adapters
- `core.adapter.grpc` - gRPC adapter implementation
- `core.adapter.rest` - REST adapter implementation

### `core.cache` - Cache Implementation
- `core.cache.api` - Internal cache APIs
- `core.cache.impl` - Cache core implementation
- `core.cache.wal` - WAL (Write-Ahead Log) components
- `core.cache.client` - Internal client implementations
- `core.cache.annotations` - Cache annotations

### Other Core Components
- `core.wal` - WAL entities (WALEntry, WALEntryBatch)
- `core.consumer` - Consumer implementations
- `core.sql` - SQL related components

## 3. `com.telcobright.examples` - Examples
**Sample implementations and test clients**

- `examples.oltp` - OLTP example implementations
  - Entity definitions
  - Test clients
  - Server examples
  - Service examples
- `examples.DBCacheWithBatching` - Example cache with batching

## Architecture Flow

```
External Clients
       ↓
[API Package]
CacheServiceProxy (2 methods only)
       ↓
[Core Package]
Internal Implementation
       ↓
[Adapters: gRPC/REST]
```

This structure ensures:
- Clean separation of concerns
- Minimal public API surface (only 2 methods)
- All complexity hidden in core
- Easy to extend with new transport adapters
- Examples separated from production code