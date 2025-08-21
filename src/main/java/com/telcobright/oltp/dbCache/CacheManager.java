package com.telcobright.oltp.dbCache;

import com.telcobright.core.wal.WALWriter;
import com.telcobright.oltp.cache.PackageAccountCache;
import com.telcobright.oltp.cache.PackageAccountReserveCache;
import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.entity.PackageAccountReserve;
import com.telcobright.oltp.entity.PackageAccountReserveDelta;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.telcobright.oltp.service.PendingStatusChecker;
import com.telcobright.oltp.service.SqlStatementCache;
import com.zaxxer.hikari.HikariDataSource;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Central cache manager that coordinates PackageAccountCache and PackageAccountReserveCache.
 * Uses the simplified architecture with separate cache classes extending EntityCache.
 */
@Startup
@ApplicationScoped
public class CacheManager {
    private static final Logger logger = LoggerFactory.getLogger(CacheManager.class);
    
    @Inject
    HikariDataSource dataSource;
    
    @Inject
    PendingStatusChecker pendingStatusChecker;
    
    @ConfigProperty(name = "db.name")
    String adminDbConfig;
    
    @Inject
    ChronicleInstance chronicleInstance;
    
    // Separate cache instances
    private PackageAccountCache packageAccountCache;
    private PackageAccountReserveCache packageAccountReserveCache;
    
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    
    @Scheduled(every = "2s", delay = 0, concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void initCacheAfterReplay() {
        if (isInitialized.get()) return;
        
        if (!pendingStatusChecker.isReplayInProgress()) {
            try {
                logger.info("Pending replay completed ✅. Initializing caches...");
                
                SqlStatementCache.initializeAll(
                    PackageAccount.class,
                    PackageAccountReserve.class
                );
                
                // Create separate cache instances
                packageAccountCache = new PackageAccountCache();
                packageAccountReserveCache = new PackageAccountReserveCache();
                
                // Initialize with datasource and WAL writer
                WALWriter walWriter = new WALWriter(chronicleInstance.getAppender());
                packageAccountCache.initialize(dataSource, walWriter);
                packageAccountReserveCache.initialize(dataSource, walWriter);
                
                // Discover and load all databases
                List<String> databases = discoverDatabases();
                for (String dbName : databases) {
                    try {
                        packageAccountCache.loadFromDatabase(dbName);
                        packageAccountReserveCache.loadFromDatabase(dbName);
                        logger.info("✅ Loaded caches for database: {}", dbName);
                    } catch (Exception e) {
                        logger.error("Failed to load caches for database: {}", dbName, e);
                    }
                }
                
                isInitialized.set(true);
                
                // Log statistics
                logger.info("✅ Caches initialized: {} accounts, {} reserves across {} databases",
                    packageAccountCache.totalSize(), 
                    packageAccountReserveCache.totalSize(), 
                    databases.size());
                
            } catch (Exception e) {
                logger.error("❌ Failed to initialize caches", e);
            }
        } else {
            logger.info("⏳ Waiting for replay to finish before initializing caches...");
        }
    }
    
    private List<String> discoverDatabases() throws SQLException {
        List<String> databases = new ArrayList<>();

        String sql = """
        SELECT schema_name 
        FROM information_schema.schemata 
        WHERE schema_name LIKE 'res\\_%'
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {

            while (rs.next()) {
                databases.add(rs.getString("schema_name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
//        if (databases.isEmpty()) {
            databases.add(adminDbConfig);
//        }
        
        logger.info("Discovered {} databases: {}", databases.size(), databases);
        return databases;
    }
    
    // ===== PackageAccount Operations =====
    
    /**
     * Get a PackageAccount from cache
     */
    public PackageAccount getPackageAccount(String dbName, Long accountId) {
        if (!isInitialized.get() || packageAccountCache == null) {
            logger.warn("Cache not initialized");
            return null;
        }
        
        PackageAccount account = packageAccountCache.get(dbName, accountId);
        
        // Lazy load if not in cache
        if (account == null && dataSource != null) {
            try {
                loadSingleAccount(dbName, accountId);
                account = packageAccountCache.get(dbName, accountId);
            } catch (Exception e) {
                logger.error("Failed to lazy load account {} from {}", accountId, dbName, e);
            }
        }
        
        return account;
    }
    
    /**
     * Get all PackageAccounts for a database
     */
    public Map<Long, PackageAccount> getAllPackageAccounts(String dbName) {
        if (!isInitialized.get() || packageAccountCache == null) {
            return new HashMap<>();
        }
        return packageAccountCache.getAll(dbName);
    }
    
    /**
     * Update PackageAccount balances (batch)
     */
    public void updatePackageAccounts(List<PackageAccDelta> deltas) {
        if (!isInitialized.get() || packageAccountCache == null) {
            logger.warn("Cache not initialized");
            return;
        }
        
        for (PackageAccDelta delta : deltas) {
            boolean success = packageAccountCache.updateBalance(
                delta.getDbName(), 
                delta.getAccountId(), 
                delta.getAmount()
            );
            
            if (!success) {
                logger.warn("Failed to update balance for account {} in {}", 
                    delta.getAccountId(), delta.getDbName());
            }
        }
    }
    
    /**
     * Delete a PackageAccount
     */
    public void deletePackageAccount(String dbName, Long accountId) {
        if (!isInitialized.get() || packageAccountCache == null) {
            logger.warn("Cache not initialized");
            return;
        }
        packageAccountCache.delete(dbName, accountId);
    }
    
    /**
     * Update or insert a PackageAccount in cache (for Kafka listener)
     */
    public void updateAccountCache(String dbName, PackageAccount account) {
        if (!isInitialized.get() || packageAccountCache == null) {
            logger.warn("Cache not initialized");
            return;
        }
        
        if (account == null || account.getId() == null) {
            logger.warn("Cannot update cache with null account or null ID");
            return;
        }
        
        packageAccountCache.put(dbName, account);
        logger.debug("Updated account {} in cache for {}", account.getId(), dbName);
    }
    
    // ===== PackageAccountReserve Operations =====
    
    /**
     * Get a Reserve from cache
     */
    public PackageAccountReserve getReserve(String dbName, Long reserveId) {
        if (!isInitialized.get() || packageAccountReserveCache == null) {
            logger.warn("Cache not initialized");
            return null;
        }
        return packageAccountReserveCache.get(dbName, reserveId);
    }
    
    /**
     * Find active reserve by package account ID
     */
    public PackageAccountReserve findActiveReserveByPackageAccountId(String dbName, Long packageAccountId) {
        if (!isInitialized.get() || packageAccountReserveCache == null) {
            logger.warn("Cache not initialized");
            return null;
        }
        
        return packageAccountReserveCache.findActiveReserveByPackageAccountId(dbName, packageAccountId);
    }
    
    /**
     * Update reserves (batch)
     */
    public void updateReserves(List<PackageAccountReserveDelta> deltas) {
        if (!isInitialized.get() || packageAccountReserveCache == null) {
            logger.warn("Cache not initialized");
            return;
        }
        
        for (PackageAccountReserveDelta delta : deltas) {
            if (delta.getAmount().signum() > 0) {
                // Create new reservation
                packageAccountReserveCache.createReservation(
                    delta.getDbName(),
                    delta.getAccountId(),
                    delta.getSessionId(),
                    delta.getAmount()
                );
            } else {
                // Release existing reservation
                // Find the reserve by account ID
                PackageAccountReserve reserve = packageAccountReserveCache
                    .findActiveReserveByPackageAccountId(delta.getDbName(), delta.getAccountId());
                    
                if (reserve != null) {
                    packageAccountReserveCache.releaseReservation(
                        delta.getDbName(),
                        reserve.getId(),
                        delta.getAmount().abs()
                    );
                }
            }
        }
    }
    
    /**
     * Delete a reserve
     */
    public void deleteReserve(String dbName, Long reserveId) {
        if (!isInitialized.get() || packageAccountReserveCache == null) {
            logger.warn("Cache not initialized");
            return;
        }
        packageAccountReserveCache.delete(dbName, reserveId);
    }
    
    /**
     * Load a single account from database (lazy loading)
     */
    private void loadSingleAccount(String dbName, Long accountId) throws SQLException {
        String sql = String.format(
            "SELECT * FROM %s.packageaccount WHERE id_packageaccount = ?", 
            dbName
        );
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setLong(1, accountId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    PackageAccount account = new PackageAccount();
                    account.setId(rs.getLong("id_packageaccount"));
                    account.setPackagePurchaseId(rs.getLong("id_PackagePurchase"));
                    account.setName(rs.getString("name"));
                    account.setLastAmount(rs.getBigDecimal("lastAmount"));
                    account.setBalanceBefore(rs.getBigDecimal("balanceBefore"));
                    account.setBalanceAfter(rs.getBigDecimal("balanceAfter"));
                    account.setUom(rs.getString("uom"));
                    account.setIsSelected(rs.getBoolean("isSelected"));
                    
                    packageAccountCache.put(dbName, account);
                }
            }
        }
    }
    
    /**
     * Check if initialized
     */
    public boolean isInitialized() {
        return isInitialized.get();
    }
    
    /**
     * Get cache statistics
     */
    public String getCacheStats() {
        if (!isInitialized.get()) {
            return "Cache not initialized";
        }
        
        return String.format("Accounts: %d, Reserves: %d, Databases: %s",
            packageAccountCache.totalSize(),
            packageAccountReserveCache.totalSize(),
            packageAccountCache.getDatabaseNames()
        );
    }
    
    // Expose the individual caches for direct access if needed
    public PackageAccountCache getPackageAccountCache() {
        return packageAccountCache;
    }
    
    public PackageAccountReserveCache getPackageAccountReserveCache() {
        return packageAccountReserveCache;
    }
}