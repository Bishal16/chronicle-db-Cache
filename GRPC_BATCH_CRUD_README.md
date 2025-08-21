# gRPC Batch CRUD Service

## Overview
This service provides atomic batch CRUD operations for PackageAccount and PackageAccountReserve entities via gRPC.

## Features
- Atomic batch transactions (all operations succeed or all fail)
- Support for multiple entity types
- Delta-based updates and deletes
- Full CRUD operations (INSERT, UPDATE, DELETE)
- Detailed logging for debugging

## Entity Types Supported
1. **PackageAccount** - Main account entity (matches Java entity structure)
   - `id`: Account ID (maps to id_packageaccount)
   - `package_purchase_id`: Package purchase ID
   - `name`: Account name
   - `last_amount`: Last transaction amount (string for BigDecimal)
   - `balance_before`: Balance before transaction
   - `balance_after`: Current balance
   - `uom`: Unit of measure
   - `is_selected`: Selection flag

2. **PackageAccountReserve** - Reserve/reservation entity (matches Java entity structure)
   - `id`: Reserve ID
   - `package_account_id`: Associated account ID
   - `session_id`: Session identifier
   - `reserved_amount`: Reserved amount (string for BigDecimal)
   - `reserved_at`: Reservation timestamp (ISO 8601)
   - `released_at`: Release timestamp (ISO 8601)
   - `status`: RESERVED, RELEASED, or EXPIRED
   - `current_reserve`: Current reserve amount

## Operation Types
- `INSERT` - Insert new entity
- `UPDATE` - Update existing entity  
- `DELETE` - Delete entity
- `UPDATE_DELTA` - Apply delta update (e.g., balance changes)
- `DELETE_DELTA` - Delete by ID only

## Delta Types (Following Java Class Naming)
- **PackageAccountDelta** - For account balance updates
  - `db_name`: Database name
  - `account_id`: Account ID
  - `amount`: Delta amount (positive to deduct from balance)
  
- **PackageAccountReserveDelta** - For reserve updates  
  - `db_name`: Database name
  - `reserve_id`: Reserve ID
  - `amount`: Delta amount (positive to add, negative to release)
  - `session_id`: Session identifier
  
- **PackageAccountDeleteDelta** - For account deletion
  - `db_name`: Database name
  - `account_id`: Account ID to delete
  
- **PackageAccountReserveDeleteDelta** - For reserve deletion
  - `db_name`: Database name
  - `reserve_id`: Reserve ID to delete

## Testing Instructions

### 1. Start the Quarkus Server
```bash
./mvnw quarkus:dev
```
The gRPC server will start on port 9000.

### 2. Run the Test Client
In a new terminal:
```bash
./run-grpc-client.sh
```

Or directly with Maven:
```bash
mvn exec:java \
  -Dexec.mainClass="com.telcobright.oltp.example.BatchCrudGrpcClient" \
  -Dexec.args="localhost 9000"
```

### 3. Check the Logs
The server will log all received operations with details:
- Transaction ID
- Database name
- Each operation's details
- Success/failure status
- Rollback information (if applicable)

## Example Batch Request Structure
```
BatchRequest {
  transaction_id: "TXN_unique_id"
  db_name: "res_1"
  operations: [
    {
      operation_type: INSERT
      entity_type: PACKAGE_ACCOUNT
      package_account: { ... }
    },
    {
      operation_type: UPDATE_DELTA
      entity_type: PACKAGE_ACCOUNT
      package_account_delta: {
        db_name: "res_1"
        account_id: 1001
        amount: "50.25"
      }
    },
    {
      operation_type: DELETE_DELTA
      entity_type: PACKAGE_ACCOUNT_RESERVE
      package_account_reserve_delete_delta: {
        db_name: "res_1"
        reserve_id: 888
      }
    }
  ]
}
```

## Response Structure
```
BatchResponse {
  success: true/false
  message: "Status message"
  transaction_id: "TXN_unique_id"
  operations_processed: count
  results: [
    {
      operation_index: 0
      success: true
      message: "Operation details"
      entity_type: PACKAGE_ACCOUNT
      operation_type: INSERT
    }
  ]
  error: {  // Only if failed
    code: "ERROR_CODE"
    message: "Error details"
    failed_operation_index: index
  }
}
```

## Client Test Scenarios
The example client demonstrates:
1. **Mixed Batch** - Multiple operations of different types
2. **Failing Batch** - Tests transaction rollback

## Current Status
- ✅ Proto definitions complete
- ✅ Server endpoint created with logging
- ✅ Client example created
- ✅ Transaction management (begin, commit, rollback)
- ⏳ Actual database operations (TODO)
- ⏳ Integration with cache system (TODO)

## Next Steps
1. Integrate with CacheManager for actual operations
2. Add validation for entity fields
3. Add metrics and monitoring
4. Add authentication/authorization
5. Performance testing with large batches