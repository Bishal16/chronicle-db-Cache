package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.dbCache.PackageAccountCache;
import com.telcobright.oltp.dbCache.PackageAccountReserveCache;
import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.entity.PackageAccountReserve;
import com.telcobright.oltp.entity.PackageAccountReserveDelta;
import com.telcobright.oltp.service.PendingStatusChecker;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prepaidaccounting.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Blocking
@GrpcService
public class PrepaidAccountingGrpcService implements PrepaidAccounting {
    @Inject
    public PrepaidAccountingGrpcService() {
    }

    @Inject
    PackageAccountCache packageAccountCache;
    
    @Inject
    PackageAccountReserveCache packageAccountReserveCache;

    @Inject
    PendingStatusChecker pendingStatusChecker;

    private static final Logger logger = LoggerFactory.getLogger(PrepaidAccountingGrpcService.class);

    @Override
    public Uni<UpdateResponse> updateBalanceThroughJdbcCache(Deltas request) {
        if (pendingStatusChecker.isReplayInProgress()) {
            return Uni.createFrom().failure(
                    new IllegalStateException("Service not ready. Pending replay in progress.")
            );
        }
        List<PackageAccDelta> deltasList = request.getDeltasList().stream()
                .map(delta -> new PackageAccDelta(
                        delta.getDbName(),
                        delta.getIdPackageAcc(),
                        BigDecimal.valueOf(delta.getAmount())
                ))
                .toList();

        if(deltasList.isEmpty() || packageAccountCache.getAccountCache().isEmpty())
            return Uni.createFrom().item(() -> UpdateResponse.newBuilder()
                .setSuccess(false)
                .setMessage("deltas or cache is empty ")
                .build());

        try{
            for (PackageAccDelta delta : deltasList) {
                PackageAccount targetAccount = packageAccountCache.getAccountCache().get(delta.dbName).get(delta.accountId);
                if (targetAccount == null) {
                    throw new IllegalArgumentException("Account not found: " + delta.accountId + ", Db name : " + delta.dbName);
                }

                BigDecimal currentBalance = targetAccount.getBalanceAfter();
                if (currentBalance.compareTo(delta.amount) < 0) {
                    throw new IllegalStateException("insufficient balance. Pkg Id : "
                            + delta.accountId + ", Db name : " + delta.dbName + " current balance :"
                            + currentBalance + ", requested balance : " + delta.amount);
                }
            }

            packageAccountCache.update(deltasList);

            return Uni.createFrom().item(() -> UpdateResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("All updates successful.")
                    .build());
        } catch (Exception e) {
            logger.error("Delta update failed", e);

            return Uni.createFrom().item(() -> UpdateResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage(e.getMessage())
                    .build());
        }
    }


    @Override
    public Uni<BalanceResponse> getBalance(GetBalanceRequest request) {
        PackageAccount targetAccount = packageAccountCache.getAccountCache().get(request.getDbName()).get(request.getIdPackageAcc());
        if(targetAccount == null)
            return Uni.createFrom().item(() -> BalanceResponse.newBuilder()
                    .setStatus(false)
                    .setBalance(0)
                    .build());
        logger.info("Requested Package info: balance = {}, idPackageAcc = {} dbName = {}"
                , request.getIdPackageAcc(), targetAccount.getBalanceAfter(), request.getDbName());
        return Uni.createFrom().item(() -> BalanceResponse.newBuilder()
                .setStatus(true)
                .setBalance(targetAccount.getBalanceAfter().doubleValue())
                .build());
    }

    @Override
    public Uni<DeletePackageResponse> deletePackageAccount(DeletePackageRequest request) {
        if (pendingStatusChecker.isReplayInProgress()) {
            return Uni.createFrom().failure(
                    new IllegalStateException("Service not ready. Pending replay in progress.")
            );
        }

        String dbName = request.getDbName();
        Long accountId = request.getIdPackageAcc();

        if (dbName == null || dbName.isEmpty() || accountId == null) {
            return Uni.createFrom().item(() -> DeletePackageResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Invalid request: dbName and idPackageAcc are required")
                    .build());
        }

        try {
            // Check if account exists in cache
            ConcurrentHashMap<Long, PackageAccount> dbCache = packageAccountCache.getAccountCache().get(dbName);
            if (dbCache == null || !dbCache.containsKey(accountId)) {
                return Uni.createFrom().item(() -> DeletePackageResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Package account not found: id=" + accountId + ", db=" + dbName)
                        .build());
            }

            // Perform delete through cache (writes to WAL and removes from cache)
            packageAccountCache.delete(dbName, accountId);

            logger.info("✅ Deleted package account via gRPC: accountId={}, dbName={}", accountId, dbName);

            return Uni.createFrom().item(() -> DeletePackageResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Package account deleted successfully")
                    .build());

        } catch (Exception e) {
            logger.error("❌ Failed to delete package account", e);
            return Uni.createFrom().item(() -> DeletePackageResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error: " + e.getMessage())
                    .build());
        }
    }

    // PackageAccountReserve operations
    
    @Override
    public Uni<ReserveResponse> reserveBalance(ReserveRequest request) {
        if (pendingStatusChecker.isReplayInProgress()) {
            return Uni.createFrom().failure(
                    new IllegalStateException("Service not ready. Pending replay in progress.")
            );
        }

        try {
            String dbName = request.getDbName();
            Long packageAccountId = request.getIdPackageAccount();
            String sessionId = request.getSessionId();
            BigDecimal amount = BigDecimal.valueOf(request.getAmount());

            // Check if package account exists and has sufficient balance
            ConcurrentHashMap<Long, PackageAccount> accountCache = packageAccountCache.getAccountCache().get(dbName);
            if (accountCache == null || !accountCache.containsKey(packageAccountId)) {
                return Uni.createFrom().item(() -> ReserveResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Package account not found: id=" + packageAccountId)
                        .build());
            }

            PackageAccount account = accountCache.get(packageAccountId);
            if (account.getBalanceAfter().compareTo(amount) < 0) {
                return Uni.createFrom().item(() -> ReserveResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Insufficient balance. Available: " + account.getBalanceAfter() + ", Requested: " + amount)
                        .build());
            }

            // Create new reserve or update existing one
            PackageAccountReserve existingReserve = packageAccountReserveCache.findActiveReserveByPackageAccountId(dbName, packageAccountId);
            
            Long reserveId;
            if (existingReserve != null) {
                reserveId = existingReserve.getId();
                // Add to existing reservation
                PackageAccountReserveDelta delta = new PackageAccountReserveDelta(dbName, reserveId, amount, sessionId);
                packageAccountReserveCache.update(java.util.List.of(delta));
            } else {
                // Create new reservation (would need to generate ID - simplified here)
                reserveId = System.currentTimeMillis(); // In production, use proper ID generation
                PackageAccountReserve newReserve = new PackageAccountReserve();
                newReserve.setId(reserveId);
                newReserve.setPackageAccountId(packageAccountId);
                newReserve.setSessionId(sessionId);
                newReserve.setReservedAmount(amount);
                newReserve.setCurrentReserve(amount);
                newReserve.setReservedAt(java.time.LocalDateTime.now());
                newReserve.setStatus("RESERVED");
                
                packageAccountReserveCache.updateReserveCache(dbName, newReserve);
                
                // Write to WAL
                PackageAccountReserveDelta delta = new PackageAccountReserveDelta(dbName, reserveId, amount, sessionId);
                packageAccountReserveCache.update(java.util.List.of(delta));
            }

            logger.info("✅ Reserved amount {} for packageAccountId={}, reserveId={}", amount, packageAccountId, reserveId);

            return Uni.createFrom().item(() -> ReserveResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Reservation successful")
                    .setReserveId(reserveId)
                    .build());

        } catch (Exception e) {
            logger.error("❌ Failed to reserve balance", e);
            return Uni.createFrom().item(() -> ReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error: " + e.getMessage())
                    .build());
        }
    }

    @Override
    public Uni<ReleaseReserveResponse> releaseReserve(ReleaseReserveRequest request) {
        if (pendingStatusChecker.isReplayInProgress()) {
            return Uni.createFrom().failure(
                    new IllegalStateException("Service not ready. Pending replay in progress.")
            );
        }

        try {
            String dbName = request.getDbName();
            Long reserveId = request.getReserveId();
            BigDecimal releaseAmount = BigDecimal.valueOf(request.getAmount());
            String sessionId = request.getSessionId();

            // Find the reserve
            ConcurrentHashMap<Long, PackageAccountReserve> reserveCache = packageAccountReserveCache.getReserveCache().get(dbName);
            if (reserveCache == null || !reserveCache.containsKey(reserveId)) {
                return Uni.createFrom().item(() -> ReleaseReserveResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Reserve not found: id=" + reserveId)
                        .build());
            }

            PackageAccountReserve reserve = reserveCache.get(reserveId);
            if (!"RESERVED".equals(reserve.getStatus())) {
                return Uni.createFrom().item(() -> ReleaseReserveResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Reserve is not in RESERVED status: " + reserve.getStatus())
                        .build());
            }

            // Create negative delta to release
            PackageAccountReserveDelta releaseDelta = new PackageAccountReserveDelta(
                    dbName, reserveId, releaseAmount.negate(), sessionId);
            
            packageAccountReserveCache.update(java.util.List.of(releaseDelta));

            logger.info("✅ Released amount {} from reserveId={}", releaseAmount, reserveId);

            return Uni.createFrom().item(() -> ReleaseReserveResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Release successful")
                    .setRemainingReserve(reserve.getCurrentReserve().subtract(releaseAmount).doubleValue())
                    .build());

        } catch (Exception e) {
            logger.error("❌ Failed to release reserve", e);
            return Uni.createFrom().item(() -> ReleaseReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error: " + e.getMessage())
                    .build());
        }
    }

    @Override
    public Uni<GetReserveResponse> getReserve(GetReserveRequest request) {
        try {
            String dbName = request.getDbName();
            Long reserveId = request.getReserveId();

            ConcurrentHashMap<Long, PackageAccountReserve> reserveCache = packageAccountReserveCache.getReserveCache().get(dbName);
            if (reserveCache == null || !reserveCache.containsKey(reserveId)) {
                return Uni.createFrom().item(() -> GetReserveResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Reserve not found: id=" + reserveId)
                        .build());
            }

            PackageAccountReserve reserve = reserveCache.get(reserveId);
            
            return Uni.createFrom().item(() -> GetReserveResponse.newBuilder()
                    .setSuccess(true)
                    .setReserveId(reserve.getId())
                    .setPackageAccountId(reserve.getPackageAccountId())
                    .setSessionId(reserve.getSessionId())
                    .setCurrentReserve(reserve.getCurrentReserve().doubleValue())
                    .setStatus(reserve.getStatus())
                    .build());

        } catch (Exception e) {
            logger.error("❌ Failed to get reserve", e);
            return Uni.createFrom().item(() -> GetReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error: " + e.getMessage())
                    .build());
        }
    }

    @Override
    public Uni<DeleteReserveResponse> deleteReserve(DeleteReserveRequest request) {
        if (pendingStatusChecker.isReplayInProgress()) {
            return Uni.createFrom().failure(
                    new IllegalStateException("Service not ready. Pending replay in progress.")
            );
        }

        try {
            String dbName = request.getDbName();
            Long reserveId = request.getReserveId();

            ConcurrentHashMap<Long, PackageAccountReserve> reserveCache = packageAccountReserveCache.getReserveCache().get(dbName);
            if (reserveCache == null || !reserveCache.containsKey(reserveId)) {
                return Uni.createFrom().item(() -> DeleteReserveResponse.newBuilder()
                        .setSuccess(false)
                        .setMessage("Reserve not found: id=" + reserveId)
                        .build());
            }

            // Delete through cache (writes to WAL and removes from cache)
            packageAccountReserveCache.delete(dbName, reserveId);

            logger.info("✅ Deleted reserve via gRPC: reserveId={}, dbName={}", reserveId, dbName);

            return Uni.createFrom().item(() -> DeleteReserveResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Reserve deleted successfully")
                    .build());

        } catch (Exception e) {
            logger.error("❌ Failed to delete reserve", e);
            return Uni.createFrom().item(() -> DeleteReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error: " + e.getMessage())
                    .build());
        }
    }


//    @Override
//    public Uni<InsertPackageResponse> insertPackageAccount(InsertPackageRequest request) {
//        try {
//            prepaidaccounting.PackageAccount grpcPkg = request.getPkgAcc();
//
//            PackageAccount newPackageAccount = new PackageAccount(
//                    grpcPkg.getId(),
//                    grpcPkg.getPackagePurchaseId(),
//                    grpcPkg.getName(),
//                    new BigDecimal(grpcPkg.getLastAmount()),
//                    new BigDecimal(grpcPkg.getBalanceBefore()),
//                    new BigDecimal(grpcPkg.getBalanceAfter()),
//                    grpcPkg.getUom(),
//                    grpcPkg.getIsSelected()
//            );
//
//            packageAccountCache.insert(newPackageAccount);
//
//            logger.info("✅ Inserted new package account: {}", newPackageAccount);

//            return Uni.createFrom().item(() ->
//                    InsertPackageResponse.newBuilder()
//                            .setSuccess(true)
//                            .setMessage("PackageAccount inserted successfully")
//                            .build()
//            );
//        } catch (Exception e) {
//            logger.error("❌ Failed to insert package account", e);
//            return Uni.createFrom().item(() ->
//                    InsertPackageResponse.newBuilder()
//                            .setSuccess(false)
//                            .setMessage("Error: " + e.getMessage())
//                            .build()
//            );
//        }
//    }

}
