package com.telcobright.oltp.dbCache;

import com.telcobright.oltp.entity.PackageAccountReserve;
import com.telcobright.oltp.entity.PackageAccountReserveDelta;
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
import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Startup
@ApplicationScoped
public class PackageAccountReserveCache extends ChronicleQueueCache<PackageAccountReserve, PackageAccountReserveDelta> {

    @Inject
    HikariDataSource dataSource;

    @Inject
    PendingStatusChecker pendingStatusChecker;

    @ConfigProperty(name = "db.name")
    String adminDbConfig;

    @Inject
    ChronicleInstance chronicleInstanceInjected;

    private static final Logger logger = LoggerFactory.getLogger(PackageAccountReserveCache.class);

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    public PackageAccountReserveCache() {
        super("PackageAccountReserve");
    }

    @Scheduled(every = "2s", delay = 0, concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void initCacheAfterReplay() {
        if (isInitialized.get()) return;

        if (!pendingStatusChecker.isReplayInProgress()) {
            try {
                logger.info("Pending replay completed ✅. Initializing reserve cache...");

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
                logger.error("❌ Failed to initialize reserve cache", e);
            }
        } else {
            logger.info("⏳ Waiting for replay to finish before initializing reserve cache...");
        }
    }

    @Override
    protected void loadEntitiesFromDb(Connection conn, String dbName,
                                      ConcurrentHashMap<Long, PackageAccountReserve> cache) throws SQLException {
        final String sql = String.format("""
        SELECT
            CAST(NULL AS SIGNED)          AS id,
            `id_packageaccount`           AS packageAccountId,
            `channel_call_uuid`           AS sessionId,
            `reserveUnit`                 AS reservedAmount,
            CAST(NULL AS DATETIME)        AS reservedAt,
            CAST(NULL AS DATETIME)        AS releasedAt,
            'RESERVED'                    AS status,
            `reserveUnit`                 AS currentReserve
        FROM `%s`.`packageaccountreserve`
        """, dbName);

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                PackageAccountReserve reserve = mapResultSetToEntity(rs);

                // Ensure required defaults in case mapper leaves nulls
                if (reserve.getStatus() == null) {
                    reserve.setStatus("RESERVED");
                }
                if (reserve.getCurrentReserve() == null) {
                    reserve.setCurrentReserve(
                            reserve.getReservedAmount() != null ? reserve.getReservedAmount() : java.math.BigDecimal.ZERO
                    );
                }

                // Keep the map key as Long (per method signature): use packageAccountId
                long key = rs.getLong("packageAccountId");
                cache.put(key, reserve);

                logReserveInfo(reserve);
            }
        } catch (Exception e) {
            logger.error("Couldn't initialize reserve cache from db for {}", dbName, e);
            throw new RuntimeException(e);
        }
    }



    @Override
    protected PackageAccountReserve mapResultSetToEntity(ResultSet rs) throws SQLException {
        PackageAccountReserve reserve = new PackageAccountReserve();
        reserve.setPackageAccountId(rs.getLong("packageAccountId"));
        reserve.setSessionId(rs.getString("sessionId"));
        reserve.setReservedAmount(rs.getBigDecimal("reservedAmount"));
        
        Timestamp reservedAt = rs.getTimestamp("reservedAt");
        if (reservedAt != null) {
            reserve.setReservedAt(reservedAt.toLocalDateTime());
        }
        
        Timestamp releasedAt = rs.getTimestamp("releasedAt");
        if (releasedAt != null) {
            reserve.setReleasedAt(releasedAt.toLocalDateTime());
        }
        
        reserve.setStatus(rs.getString("status"));
        reserve.setCurrentReserve(rs.getBigDecimal("currentReserve"));
        return reserve;
    }

    @Override
    protected void writeDeltaToWAL(WireOut wire, PackageAccountReserveDelta delta) {
        wire.write("dbName").text(delta.getDbName());
        wire.write("reserveId").int64(delta.getAccountId());
        wire.write("amount").text(delta.getAmount().toPlainString());
        wire.write("sessionId").text(delta.getSessionId());
    }

    @Override
    protected void logDeltaApplication(PackageAccountReserveDelta delta, PackageAccountReserve entity) {
        System.out.println("\nReserve Delta Applied:");
        System.out.printf("""
                Database               = %s
                ID_PackageReserve      = %d
                Session ID             = %s
                Delta Amount           = %s
                Current Reserve        = %s
                Status                 = %s
            ----------------------------------------
            """,
                delta.getDbName(),
                delta.getAccountId(),
                delta.getSessionId(),
                delta.getAmount(),
                entity.getCurrentReserve(),
                entity.getStatus()
        );
    }

    private void logReserveInfo(PackageAccountReserve reserve) {
        logger.info("Cached PackageAccountReserve: ID={}, SessionId={}, PackageAccountId={}, " +
                "ReservedAmount={}, CurrentReserve={}, Status={}",
                reserve.getId(), reserve.getSessionId(), reserve.getPackageAccountId(),
                reserve.getReservedAmount(), reserve.getCurrentReserve(), reserve.getStatus());
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<Long, PackageAccountReserve>> getReserveCache() {
        return super.getCache();
    }
    
    public void updateReserveCache(String dbName, PackageAccountReserve reserve) {
        super.updateEntityInCache(dbName, reserve);
    }

    // Additional method to find reserves by package account ID
    public PackageAccountReserve findActiveReserveByPackageAccountId(String dbName, Long packageAccountId) {
        ConcurrentHashMap<Long, PackageAccountReserve> dbCache = getCache().get(dbName);
        if (dbCache != null) {
            return dbCache.values().stream()
                .filter(reserve -> reserve.getPackageAccountId().equals(packageAccountId))
                .filter(reserve -> "RESERVED".equals(reserve.getStatus()))
                .findFirst()
                .orElse(null);
        }
        return null;
    }

    // Method to expire old reserves
    public void expireOldReserves(int expirationMinutes) {
        LocalDateTime expirationTime = LocalDateTime.now().minusMinutes(expirationMinutes);
        
        getCache().forEach((dbName, dbCache) -> {
            dbCache.forEach((id, reserve) -> {
                if ("RESERVED".equals(reserve.getStatus()) && 
                    reserve.getReservedAt().isBefore(expirationTime)) {
                    
                    // Create a delta to release the reservation
                    PackageAccountReserveDelta releaseDelta = new PackageAccountReserveDelta(
                        dbName, 
                        id, 
                        reserve.getCurrentReserve().negate(), // negative to release
                        reserve.getSessionId()
                    );
                    
                    // Apply the release
                    update(java.util.List.of(releaseDelta));
                    logger.info("Expired reserve: id={}, sessionId={}, dbName={}", 
                        id, reserve.getSessionId(), dbName);
                }
            });
        });
    }
}