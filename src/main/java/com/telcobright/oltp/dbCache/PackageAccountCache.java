package com.telcobright.oltp.dbCache;

import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.telcobright.oltp.service.PendingStatusChecker;
import com.zaxxer.hikari.HikariDataSource;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import net.openhft.chronicle.wire.WireOut;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.quarkus.scheduler.Scheduled;

import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Startup
@ApplicationScoped
public class PackageAccountCache extends ChronicleQueueCache<PackageAccount, PackageAccDelta> {

    @Inject
    HikariDataSource dataSource;

    @Inject
    PendingStatusChecker pendingStatusChecker;

    @ConfigProperty(name = "db.name")
    String adminDbConfig;

    @Inject
    ChronicleInstance chronicleInstanceInjected;

    private static final Logger logger = LoggerFactory.getLogger(PackageAccountCache.class);

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    public PackageAccountCache() {
        super("PackageAccount");
    }


//    @Scheduled(every = "2s", delay = 0, concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
//    void initCacheAfterReplay1() {
//        if (isInitialized.get()) {
//            return;
//        }
//
//        if (!pendingStatusChecker.isReplayInProgress()) {
//            try {
//                logger.info("Pending replay completed ✅. Initializing cache...");
//                super.setDataSource(dataSource);
//                initFromDb();
//                isInitialized.set(true);
//            } catch (Exception e) {
//                logger.error("❌ Failed to initialize cache", e);
//            }
//        } else {
//            logger.info("⏳ Waiting for replay to finish before initializing cache...");
//        }
//    }

    @Scheduled(every = "2s", delay = 0, concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void initCacheAfterReplay() {
        if (isInitialized.get()) return;

        if (!pendingStatusChecker.isReplayInProgress()) {
            try {
                logger.info("Pending replay completed ✅. Initializing cache...");

                if (super.getDataSource() == null) {
                    super.setDataSource(dataSource);
                    super.setChronicleInstance(chronicleInstanceInjected);
                    super.setAdminDb(adminDbConfig);
                } else {
                    logger.info("ℹ️ DataSource already set, skipping re-initialization.");
                }

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
    protected void loadEntitiesFromDb(Connection conn, String dbName,
                                     ConcurrentHashMap<Long, PackageAccount> cache) throws SQLException {
        String sql = String.format("""
            SELECT id_packageaccount AS id, id_PackagePurchase AS packagePurchaseId,
                   name, lastAmount, balanceBefore, balanceAfter, uom, isSelected
            FROM %s.packageaccount
        """, dbName);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                PackageAccount acc = mapResultSetToEntity(rs);
                acc.setId(rs.getLong("id"));
                cache.put(acc.getId(), acc);
                logPackageAccountInfo(rs, acc);
            }
        } catch (Exception e) {
            logger.error("Couldn't initialize cache from db for {}", dbName);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected PackageAccount mapResultSetToEntity(ResultSet rs) throws SQLException {
        PackageAccount acc = new PackageAccount();
        acc.setPackagePurchaseId(rs.getLong("packagePurchaseId"));
        acc.setName(rs.getString("name"));
        acc.setLastAmount(rs.getBigDecimal("lastAmount"));
        acc.setBalanceBefore(rs.getBigDecimal("balanceBefore"));
        acc.setBalanceAfter(rs.getBigDecimal("balanceAfter"));
        acc.setUom(rs.getString("uom"));
        acc.setIsSelected(rs.getBoolean("isSelected"));
        return acc;
    }

    @Override
    protected void writeDeltaToWAL(WireOut wire, PackageAccDelta delta) {
        wire.write("dbName").text(delta.getDbName());
        wire.write("accountId").int64(delta.getAccountId());
        wire.write("amount").text(delta.getAmount().toPlainString());
    }

    @Override
    protected void logDeltaApplication(PackageAccDelta delta, PackageAccount entity) {
        System.out.println("\nReserved Amount = " + delta.getAmount());
        System.out.printf("""
                Database           = %s
                ID_PackageAccount  = %d
                Balance Before     = %s
                Balance After      = %s
            ----------------------------------------
            """,
                delta.getDbName(),
                delta.getAccountId(),
                entity.getBalanceBefore(),
                entity.getBalanceAfter()
        );
    }


//    @Override
//    protected void writeWALForInsert(PackageAccount newEntity) {
//        ExcerptAppender appender = chronicleInstance.getAppender();
//        appender.writeDocument(w -> {
//            w.write("action").int32(CrudActionType.Insert.ordinal());
//            w.write("idPackageAccount").int64(newEntity.getPackagePurchaseId());
//            w.write("packagePurchaseId").int64(newEntity.getPackagePurchaseId());
//            w.write("name").text(newEntity.getName());
//            w.write("lastAmount").text(newEntity.getLastAmount().toPlainString());
//            w.write("balanceBefore").text(newEntity.getBalanceBefore().toPlainString());
//            w.write("balanceAfter").text(newEntity.getBalanceAfter().toPlainString());
//            w.write("uom").text(newEntity.getUom());
//            w.write("isSelected").bool(Boolean.TRUE.equals(newEntity.getIsSelected()));
//        });
//    }


//    @Override
//    protected Consumer<PackageAccount> getInsertAction() {
//        return newEntity -> {
//            try {
//                pkgIdVsPkgAccountCache.put(newEntity.getId(),newEntity);
//            } catch (Exception e) {
//                throw new RuntimeException("Duplicate Entity, packageAccount [id: ]" +
//                        newEntity.getId() + " already exists in the cache.");
//            }
//        };
//        return null;
//    }

    private static void logPackageAccountInfo(ResultSet rs, PackageAccount acc) throws SQLException {
        String logMessage = String.format("Cached PackageAccount:    ID = %d    Name = %s    LastAmount = %s    BalanceBefore = %s    BalanceAfter = %s    UOM = %s",
                rs.getLong("id"), acc.getName(), acc.getLastAmount(), acc.getBalanceBefore(), acc.getBalanceAfter(), acc.getUom());
        System.out.println(logMessage);
//        logger.info(logMessage);
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Long, PackageAccount>> getAccountCache() {
        return super.getCache();
    }
    
    public void updateAccountCache(String dbName, PackageAccount packageAccount) {
        super.updateEntityInCache(dbName, packageAccount);
    }


}