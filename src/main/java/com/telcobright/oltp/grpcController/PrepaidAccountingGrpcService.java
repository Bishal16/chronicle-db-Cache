package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.dbCache.CacheManager;
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
import java.util.List;
import java.util.Map;

/**
 * Simplified gRPC service using the new generic cache system
 */
@Blocking
// @GrpcService  // Disabled temporarily for SimplePayloadTestServer
public class PrepaidAccountingGrpcService implements PrepaidAccounting {
    private static final Logger logger = LoggerFactory.getLogger(PrepaidAccountingGrpcService.class);
    
    @Inject
    CacheManager cacheManager;
    
    @Inject
    PendingStatusChecker pendingStatusChecker;
    
    @Override
    public Uni<UpdateResponse> updateBalanceThroughJdbcCache(Deltas request) {
        if (pendingStatusChecker.isReplayInProgress()) {
            return Uni.createFrom().failure(
                new IllegalStateException("Service not ready. Pending replay in progress.")
            );
        }
        
        if (!cacheManager.isInitialized()) {
            return Uni.createFrom().failure(
                new IllegalStateException("Cache not initialized yet.")
            );
        }
        
        List<PackageAccDelta> deltasList = request.getDeltasList().stream()
            .map(delta -> new PackageAccDelta(
                delta.getDbName(),
                delta.getIdPackageAcc(),
                BigDecimal.valueOf(delta.getAmount())
            ))
            .toList();
        
        if (deltasList.isEmpty()) {
            return Uni.createFrom().item(() -> UpdateResponse.newBuilder()
                .setSuccess(false)
                .setMessage("No deltas provided")
                .build());
        }
        
        try {
            // Validate all accounts exist and have sufficient balance
            for (PackageAccDelta delta : deltasList) {
                PackageAccount account = cacheManager.getPackageAccount(delta.getDbName(), delta.getAccountId());
                if (account == null) {
                    throw new IllegalArgumentException("Account not found: " + delta.getAccountId() + 
                        ", Db name: " + delta.getDbName());
                }
                
                if (account.getBalanceAfter().compareTo(delta.getAmount()) < 0) {
                    throw new IllegalStateException("Insufficient balance. Account: " + delta.getAccountId() + 
                        ", Db: " + delta.getDbName() + 
                        ", Current balance: " + account.getBalanceAfter() + 
                        ", Requested: " + delta.getAmount());
                }
            }
            
            // Apply all deltas
            cacheManager.updatePackageAccounts(deltasList);
            
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
        if (!cacheManager.isInitialized()) {
            return Uni.createFrom().item(() -> BalanceResponse.newBuilder()
                .setStatus(false)
                .setBalance(0)
                .setMessage("Cache not initialized")
                .build());
        }
        
        PackageAccount account = cacheManager.getPackageAccount(request.getDbName(), request.getIdPackageAcc());
        
        if (account == null) {
            return Uni.createFrom().item(() -> BalanceResponse.newBuilder()
                .setStatus(false)
                .setBalance(0)
                .build());
        }
        
        logger.info("Balance query: accountId={}, balance={}, db={}", 
            request.getIdPackageAcc(), account.getBalanceAfter(), request.getDbName());
        
        return Uni.createFrom().item(() -> BalanceResponse.newBuilder()
            .setStatus(true)
            .setBalance(account.getBalanceAfter().doubleValue())
            .build());
    }
    
    // deletePackageAccount is commented out in the proto file
    // Uncomment this method when the RPC is enabled in the proto
    /*
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
            PackageAccount account = cacheManager.getPackageAccount(dbName, accountId);
            if (account == null) {
                return Uni.createFrom().item(() -> DeletePackageResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Package account not found: id=" + accountId + ", db=" + dbName)
                    .build());
            }
            
            cacheManager.deletePackageAccount(dbName, accountId);
            
            logger.info("✅ Deleted package account: accountId={}, dbName={}", accountId, dbName);
            
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
    */
    
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
            PackageAccount account = cacheManager.getPackageAccount(dbName, packageAccountId);
            if (account == null) {
                return Uni.createFrom().item(() -> ReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Package account not found: id=" + packageAccountId)
                    .build());
            }
            
            if (account.getBalanceAfter().compareTo(amount) < 0) {
                return Uni.createFrom().item(() -> ReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Insufficient balance. Available: " + account.getBalanceAfter() + 
                        ", Requested: " + amount)
                    .build());
            }
            
            // Check for existing reservation
            PackageAccountReserve existingReserve = cacheManager.findActiveReserveByPackageAccountId(dbName, packageAccountId);
            
            Long reserveId;
            if (existingReserve != null) {
                reserveId = existingReserve.getId();
                // Add to existing reservation
                PackageAccountReserveDelta delta = new PackageAccountReserveDelta(
                    dbName, reserveId, amount, sessionId);
                cacheManager.updateReserves(List.of(delta));
            } else {
                // Create new reservation
                reserveId = System.currentTimeMillis(); // Simple ID generation
                
                PackageAccountReserve newReserve = new PackageAccountReserve();
                newReserve.setId(reserveId);
                newReserve.setPackageAccountId(packageAccountId);
                newReserve.setSessionId(sessionId);
                newReserve.setReservedAmount(amount);
                newReserve.setCurrentReserve(amount);
                newReserve.setReservedAt(java.time.LocalDateTime.now());
                newReserve.setStatus("RESERVED");
                
                // Insert into cache - need to add insert method to SimplifiedPackageAccountCache
                // For now, use update with positive delta
                PackageAccountReserveDelta delta = new PackageAccountReserveDelta(
                    dbName, reserveId, amount, sessionId);
                cacheManager.updateReserves(List.of(delta));
            }
            
            logger.info("✅ Reserved amount {} for packageAccountId={}, reserveId={}", 
                amount, packageAccountId, reserveId);
            
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
            PackageAccountReserve reserve = cacheManager.getReserve(dbName, reserveId);
            if (reserve == null) {
                return Uni.createFrom().item(() -> ReleaseReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Reserve not found: id=" + reserveId)
                    .build());
            }
            
            if (!"RESERVED".equals(reserve.getStatus())) {
                return Uni.createFrom().item(() -> ReleaseReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Reserve is not in RESERVED status: " + reserve.getStatus())
                    .build());
            }
            
            // Create negative delta to release
            PackageAccountReserveDelta releaseDelta = new PackageAccountReserveDelta(
                dbName, reserveId, releaseAmount.negate(), sessionId);
            
            cacheManager.updateReserves(List.of(releaseDelta));
            
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
            
            PackageAccountReserve reserve = cacheManager.getReserve(dbName, reserveId);
            if (reserve == null) {
                return Uni.createFrom().item(() -> GetReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Reserve not found: id=" + reserveId)
                    .build());
            }
            
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
            
            PackageAccountReserve reserve = cacheManager.getReserve(dbName, reserveId);
            if (reserve == null) {
                return Uni.createFrom().item(() -> DeleteReserveResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Reserve not found: id=" + reserveId)
                    .build());
            }
            
            cacheManager.deleteReserve(dbName, reserveId);
            
            logger.info("✅ Deleted reserve: reserveId={}, dbName={}", reserveId, dbName);
            
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
}