package com.telcobright.oltp.service;

import com.telcobright.oltp.dbCache.CrudActionType;
import com.telcobright.oltp.dbCache.PackageAccountCache;
import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import io.quarkus.grpc.GrpcService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import net.openhft.chronicle.queue.ExcerptAppender;
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
    ChronicleInstance chronicleInstance;

    @Inject
    PackageAccountCache packageAccountCache;

    @Override
    public Uni<UpdateResponse> updateBalanceThroughJdbcCache(Deltas request) {
        List<PackageAccDelta> deltasList = request.getItemsList().stream()
                .map(delta -> new PackageAccDelta(
                        delta.getDbName(),
                        delta.getIdPackageAcc(),
                        BigDecimal.valueOf(delta.getAmount())
                ))
                .toList();

        packageAccountCache.update(deltasList);

        int count = deltasList.size();
        return Uni.createFrom().item(() -> UpdateResponse.newBuilder()
                .setCount(count)
                .build());
    }

    //working fine
    @Override
    public Uni<UpdateResponse> updateBalance(Deltas request) {
        ExcerptAppender appender = chronicleInstance.getAppender();
        List<PackageAccDelta> deltasList = request.getItemsList().stream()
                .map(delta -> new PackageAccDelta(
                        delta.getDbName(),
                        delta.getIdPackageAcc(),
                        BigDecimal.valueOf(delta.getAmount())
                ))
                .toList();

        // Write directly to Chronicle Queue WITHOUT using update() or any helper method:
        appender.writeDocument(w -> {
            w.write("action").int32(CrudActionType.Insert.ordinal());
            w.write("size").int32(deltasList.size());
            for (PackageAccDelta delta : deltasList) {
                w.write("dbName").text(delta.dbName);
                w.write("accountId").int64(delta.accountId);
                w.write("amount").text(delta.amount.toPlainString());
            }
        });
        System.out.println("message written to the queue. delta count : " + deltasList.size() );

        int count = deltasList.size();
        return Uni.createFrom().item(() -> UpdateResponse.newBuilder()
                .setCount(count)
                .build());
    }


    @Override
    public Uni<GetBalanceResponse> getBalance(GetBalanceRequest request) {
        return Uni.createFrom().item(() -> GetBalanceResponse.newBuilder()
                .setStatus("SUCCESS")
                .setBalance(0)
                .build());
    }
}
