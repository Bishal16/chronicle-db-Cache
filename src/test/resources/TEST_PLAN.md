# Universal Cache Manager Test Plan

## Overview
This document outlines comprehensive test scenarios for the Universal Cache Manager, focusing on real-world, human-like integration tests rather than unit tests. All tests will use actual MySQL database connections and real Chronicle Queue instances.

## Test Environment Setup
- MySQL Database: 127.0.0.1:3306 (root/123456)
- Chronicle Queue: File-based WAL storage
- Test Entities: PackageAccount, PackageAccountReserve, Customer, Order, Transaction

## Test Categories

### 1. Basic CRUD Operations Through Cache
**Purpose**: Verify fundamental cache operations work correctly with database synchronization

#### Test Scenarios:
- **Create and retrieve customer profile**
  - Insert new customer with full details
  - Verify data exists in both cache and database
  - Retrieve by ID and validate all fields match

- **Update product inventory levels**
  - Start with initial inventory count
  - Perform multiple updates (increment/decrement)
  - Verify final state matches expected value
  
- **Delete expired session records**
  - Create multiple session records with timestamps
  - Delete sessions older than threshold
  - Verify only active sessions remain

- **Bulk insert batch of orders**
  - Insert 1000 orders in single batch
  - Verify all orders are queryable
  - Check database record count matches

### 2. Multi-Entity Transactional Operations
**Purpose**: Test atomic operations across multiple entity types

#### Test Scenarios:
- **E-commerce checkout flow**
  - Deduct from inventory
  - Create order record
  - Update customer balance
  - Create shipping record
  - All in single transaction - verify all-or-nothing

- **Bank transfer between accounts**
  - Debit source account
  - Credit destination account
  - Create transaction log entry
  - Update account reserves
  - Rollback on insufficient funds

- **Hotel room booking**
  - Check room availability
  - Create reservation
  - Update room status
  - Process payment
  - Update guest profile
  - Handle overbooking scenario

- **Telecom package activation**
  - Create package_account entry
  - Allocate package_account_reserve
  - Update customer subscription
  - Generate billing record
  - Activate services

### 3. Concurrent Access Patterns
**Purpose**: Validate thread-safety and consistency under concurrent load

#### Test Scenarios:
- **Multiple threads updating same account balance**
  - 10 threads incrementing balance by 100
  - Verify final balance = initial + 1000
  - No lost updates

- **Parallel order processing**
  - 50 concurrent orders for same product
  - Limited inventory (30 items)
  - Verify exactly 30 orders succeed
  - 20 orders properly rejected

- **Reader-writer scenario**
  - 5 threads continuously reading account
  - 2 threads performing updates
  - Readers always see consistent data
  - No dirty reads

- **Competing reservations**
  - Multiple users booking same resource
  - Only one should succeed
  - Others get proper conflict response

### 4. Cache Consistency & Synchronization
**Purpose**: Ensure cache stays synchronized with database

#### Test Scenarios:
- **Direct database modification detection**
  - Update record directly in MySQL
  - Trigger cache refresh
  - Verify cache reflects database state

- **Cache eviction and reload**
  - Fill cache to capacity
  - Trigger eviction of old entries
  - Access evicted entry
  - Verify reload from database

- **Multi-instance cache coherence**
  - Run two cache instances
  - Update through instance A
  - Verify instance B sees update (via WAL)

- **Stale data handling**
  - Set short TTL on entries
  - Wait for expiration
  - Access expired entry
  - Verify fresh load from database

### 5. Error Handling & Recovery
**Purpose**: Test resilience and recovery mechanisms

#### Test Scenarios:
- **Database connection failure**
  - Start transaction
  - Kill database connection mid-transaction
  - Verify proper rollback
  - Verify cache unchanged
  - Restore connection and retry

- **WAL corruption handling**
  - Corrupt WAL file deliberately
  - Start cache initialization
  - Verify graceful degradation
  - Verify recovery mechanisms

- **Out of memory scenario**
  - Insert entries until memory pressure
  - Verify graceful handling
  - Check eviction policies work

- **Constraint violation handling**
  - Insert duplicate primary key
  - Insert with foreign key violation
  - Update with check constraint failure
  - Verify cache remains consistent

### 6. Performance & Scalability Tests
**Purpose**: Validate performance under various loads

#### Test Scenarios:
- **Sustained write throughput**
  - Continuous inserts for 5 minutes
  - Measure operations per second
  - Monitor memory usage
  - Check for degradation

- **Read-heavy workload**
  - 95% reads, 5% writes
  - Measure cache hit ratio
  - Verify read performance advantage

- **Large batch processing**
  - Process 10,000 entry batch
  - Measure total time
  - Verify transaction boundaries

- **Mixed entity type operations**
  - Randomly mix operations on 5 entity types
  - Measure throughput per entity type
  - Check for bottlenecks

### 7. Complex Business Scenarios
**Purpose**: Test real-world business logic patterns

#### Test Scenarios:
- **Subscription renewal workflow**
  - Check expiring subscriptions
  - Process renewal payment
  - Extend subscription period
  - Update service entitlements
  - Send notifications

- **Inventory reorder point**
  - Monitor stock levels
  - Trigger reorder when below threshold
  - Create purchase order
  - Update expected delivery

- **Customer loyalty points**
  - Calculate points from purchase
  - Apply tier multipliers
  - Update lifetime totals
  - Check for tier upgrades

- **Rate limiting implementation**
  - Track API calls per customer
  - Enforce limits per time window
  - Reset counters periodically
  - Handle burst allowances

### 8. Data Integrity Tests
**Purpose**: Ensure data integrity across operations

#### Test Scenarios:
- **Cascade delete operations**
  - Delete parent entity
  - Verify child entities handled correctly
  - Check referential integrity

- **Unique constraint enforcement**
  - Attempt duplicate inserts
  - Verify proper rejection
  - Check cache consistency

- **Field validation**
  - Insert with null required fields
  - Insert with invalid data types
  - Update with constraint violations

- **Transaction isolation levels**
  - Test different isolation levels
  - Verify expected behavior
  - Check for phantom reads

### 9. Edge Cases & Boundary Conditions
**Purpose**: Test unusual but possible scenarios

#### Test Scenarios:
- **Empty batch processing**
  - Submit empty WAL batch
  - Verify graceful handling

- **Extremely large entity**
  - Create entity with 1000 fields
  - Test storage and retrieval

- **Unicode and special characters**
  - Use various character sets
  - Emoji in text fields
  - Verify proper encoding

- **Maximum key length**
  - Test with very long keys
  - Composite keys with many fields

- **Zero, negative, and MAX values**
  - Test numeric boundaries
  - Date/time edge cases
  - String length limits

### 10. WAL-Specific Scenarios
**Purpose**: Test Write-Ahead Log functionality

#### Test Scenarios:
- **WAL replay after crash**
  - Simulate crash with pending WAL entries
  - Restart and verify replay
  - Check data consistency

- **WAL file rotation**
  - Fill WAL to rotation point
  - Verify seamless rotation
  - Check old file cleanup

- **WAL batch atomicity**
  - Large batch with failure midway
  - Verify entire batch rolled back
  - WAL entry marked as failed

- **WAL compression benefits**
  - Measure with/without compression
  - Check CPU vs disk trade-off

### 11. Cross-Database Operations
**Purpose**: Test operations spanning multiple databases

#### Test Scenarios:
- **Cross-database transaction**
  - Update entities in DB1 and DB2
  - Verify atomic commit
  - Test rollback scenarios

- **Database migration**
  - Move entity from one DB to another
  - Maintain consistency
  - Update references

- **Federated queries**
  - Join data from multiple databases
  - Aggregate across databases

### 12. Monitoring & Observability
**Purpose**: Verify monitoring capabilities

#### Test Scenarios:
- **Metrics collection**
  - Verify all metrics update correctly
  - Cache hit/miss ratios
  - Operation latencies
  - Queue depths

- **Health check endpoints**
  - Verify health status accuracy
  - Test various failure conditions
  - Check recovery detection

- **Audit trail**
  - Verify all operations logged
  - Check log completeness
  - Test audit query capabilities

## Test Execution Strategy

### Setup for Each Test
1. Clean database state
2. Fresh cache instance
3. Clear WAL files
4. Initialize test data

### Validation Methods
1. Direct database queries for verification
2. Cache API queries
3. WAL file inspection
4. Metric assertions
5. Log analysis

### Performance Baselines
- Single insert: < 10ms
- Batch of 1000: < 1 second
- Cache hit: < 1ms
- Cache miss: < 50ms
- Transaction rollback: < 100ms

## Test Data Patterns

### Standard Test Entities
1. **Customer**: id, name, email, balance, created_at, status
2. **Order**: id, customer_id, total, status, items, timestamp
3. **Product**: id, name, price, inventory, category
4. **Transaction**: id, from_account, to_account, amount, type, timestamp
5. **Session**: id, user_id, token, expires_at, data

### Data Volumes for Testing
- Small: 10-100 records
- Medium: 1,000-10,000 records
- Large: 100,000-1,000,000 records
- Stress: Maximum until failure

## Success Criteria
- All CRUD operations maintain consistency
- Transactions are truly atomic
- No data loss under concurrent access
- Performance meets baselines
- Graceful handling of all error cases
- WAL provides complete recovery
- Cache provides measurable performance benefit