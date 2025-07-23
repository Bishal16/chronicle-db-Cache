package com.telcobright.oltp.grpcController;

import com.telcobright.oltp.dbCache.PackageAccountCache;
import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
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

@Blocking
@GrpcService
public class PrepaidAccountingGrpcService implements PrepaidAccounting {
    @Inject
    public PrepaidAccountingGrpcService() {
    }

    @Inject
    PackageAccountCache packageAccountCache;

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
                PackageAccount targetAccount = packageAccountCache.getAccountCache().get(delta.accountId);
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
        PackageAccount targetAccount = packageAccountCache.getAccountCache().get(request.getIdPackageAcc());
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
}
