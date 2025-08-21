case 1: //update packeageAccuont
op2 + op3

case 2: //update packeageAccuont
op2 + op4

case 3: //delete packeageAccuontReserve
op5




1. Operation 1
    - Type: INSERT
    - Entity: PACKAGE_ACCOUNT
    - Payload: PackageAccount with ID 1001
2. Operation 2
   - Type: UPDATE_DELTA
   - Entity: PACKAGE_ACCOUNT
   - Payload: PackageAccountDelta for account ID 1001
3. Operation 3
   - Type: INSERT
   - Entity: PACKAGE_ACCOUNT_RESERVE
   - Payload: PackageAccountReserve with ID 3001
4. Operation 4
   - Type: UPDATE_DELTA
   - Entity: PACKAGE_ACCOUNT_RESERVE
   - Payload: PackageAccountReserveDelta for reserve ID 3001

5. Operation 6
   - Type: DELETE_DELTA
   - Entity: PACKAGE_ACCOUNT_RESERVE
   - Payload: PackageAccountReserveDeleteDelta for reserve ID 888

The server successfully received all 6 operations in a single batch transaction targeting the res_1 database.
