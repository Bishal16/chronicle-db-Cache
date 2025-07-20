package com.telcobright.oltp.dbCache;

import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.telcobright.oltp.service.PendingStatusChecker;
import com.zaxxer.hikari.HikariDataSource;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.quarkus.scheduler.Scheduled;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Startup
@ApplicationScoped
public class PackageAccountCache extends JdbcCache<Long, PackageAccount, List<PackageAccDelta>> {

    @Inject
    HikariDataSource dataSource;

    @Inject
    ChronicleInstance chronicleInstance;

    @Inject
    PendingStatusChecker pendingStatusChecker;

    private static final Logger logger = LoggerFactory.getLogger(PackageAccountCache.class);

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    public PackageAccountCache() {
        super();
    }


    @Scheduled(every = "1s", delay = 0, concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void initCacheAfterReplay() {
        if (isInitialized.get()) {
            return;
        }

        if (!pendingStatusChecker.isReplayInProgress()) {
            try {
                logger.info("Pending replay completed ✅. Initializing cache...");
                super.setDataSource(dataSource);
                initFromDb();
                isInitialized.set(true);
            } catch (Exception e) {
                logger.error("❌ Failed to initialize cache", e);
            }
        } else {
            logger.info("⏳ Waiting for replay to finish before initializing cache...");
        }
    }


    @Override
    public void initFromDb() throws SQLException {
        try (Connection conn = getConnection()) {
            String sql = """
            SELECT id_packageaccount AS id, id_PackagePurchase AS packagePurchaseId,
                   name, lastAmount, balanceBefore, balanceAfter, uom, isSelected
            FROM packageaccount
        """;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        PackageAccount acc = new PackageAccount();
//                        acc.setId(rs.getLong("id"));  // uncommented
                        acc.setPackagePurchaseId(rs.getLong("packagePurchaseId"));
                        acc.setName(rs.getString("name"));
                        acc.setLastAmount(rs.getBigDecimal("lastAmount"));
                        acc.setBalanceBefore(rs.getBigDecimal("balanceBefore"));
                        acc.setBalanceAfter(rs.getBigDecimal("balanceAfter"));
                        acc.setUom(rs.getString("uom"));
                        acc.setIsSelected(rs.getBoolean("isSelected"));

                        pkgIdVsPkgAccountCache.put(rs.getLong("id"), acc);

                        logPackageAccountInfo(rs, acc);

                    }
                    logger.info("Cache initialized successfully. Account count: {}", pkgIdVsPkgAccountCache.size());
                }
                catch (Exception e) {
                    logger.error("Couldn't initialize cache from db");
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    protected void writeWALForUpdate(List<PackageAccDelta> entries) {
        ExcerptAppender appender = chronicleInstance.getAppender();
        appender.writeDocument(w -> {
            w.write("action").int32(CrudActionType.Update.ordinal());
            w.write("size").int32(entries.size());
            for (PackageAccDelta entry : entries) {
                w.write("dbName").text(entry.dbName);
                w.write("accountId").int64(entry.accountId);
                w.write("amount").text(entry.amount.toPlainString());
            }
        });
        long lastIndex = appender.lastIndexAppended();
        System.out.println("Wrote WAL entry at index: " + lastIndex);
    }

    @Override
    protected Consumer<List<PackageAccDelta>> updateCache() {
        return packageAccDeltas -> {
            for (PackageAccDelta delta : packageAccDeltas) {
                PackageAccount targetAcc= pkgIdVsPkgAccountCache.get(delta.accountId);
                if(targetAcc==null){
                   throw new RuntimeException("Package account [id: ]" +delta.accountId +
                           " not found in cache");
                }

                targetAcc.applyDelta(delta.amount);

                System.out.println("\nReserved Amount = " + delta.amount);


                System.out.printf("""
                        Database           = %s
                        ID_PackageAccount  = %d
                        Balance Before     = %s
                        Balance After      = %s
                    ----------------------------------------
                    """,
                        delta.dbName,
                        delta.accountId,
                        targetAcc.getBalanceBefore(),
                        targetAcc.getBalanceAfter()
                );

            }
        };
    }

    @Override
    protected void writeWALForInsert(PackageAccount newEntity) {
        ExcerptAppender appender = chronicleInstance.getAppender();
        appender.writeDocument(w -> {
            w.write("action").int32(CrudActionType.Insert.ordinal());
            w.write("packagePurchaseId").int64(newEntity.getPackagePurchaseId());
            w.write("name").text(newEntity.getName());
            w.write("lastAmount").text(newEntity.getLastAmount().toPlainString());
            w.write("balanceBefore").text(newEntity.getBalanceBefore().toPlainString());
            w.write("balanceAfter").text(newEntity.getBalanceAfter().toPlainString());
            w.write("uom").text(newEntity.getUom());
            w.write("isSelected").bool(Boolean.TRUE.equals(newEntity.getIsSelected()));
        });
    }


    @Override
    protected Consumer<PackageAccount> getInsertAction() {
        return newEntity -> {
            try {
                pkgIdVsPkgAccountCache.put(newEntity.getId(),newEntity);
            } catch (Exception e) {
                throw new RuntimeException("Duplicate Entity, packageAccount [id: ]" +
                        newEntity.getId() + " already exists in the cache.");
            }
        };
    }

    private static void logPackageAccountInfo(ResultSet rs, PackageAccount acc) throws SQLException {
        String logMessage = String.format("""
        Cached PackageAccount:
            ID              = %d
            Name            = %s
            LastAmount      = %s
            BalanceBefore   = %s
            BalanceAfter    = %s
            UOM             = %s
        ----------------------------------------
        """,
                rs.getLong("id"),
                acc.getName(),
                acc.getLastAmount(),
                acc.getBalanceBefore(),
                acc.getBalanceAfter(),
                acc.getUom()
        );

        logger.info(logMessage);
    }

}