package com.telcobright.oltp.cache;

import com.telcobright.core.cache.EntityCache;
import com.telcobright.oltp.entity.PackageAccountReserve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Cache for PackageAccountReserve entities.
 * Structure: ConcurrentHashMap<dbname, ConcurrentHashMap<Long, PackageAccountReserve>>
 */
public class PackageAccountReserveCache extends EntityCache<Long, PackageAccountReserve> {
    private static final Logger logger = LoggerFactory.getLogger(PackageAccountReserveCache.class);
    
    /**
     * Create a new PackageAccountReserve cache
     */
    public PackageAccountReserveCache() {
        super(PackageAccountReserve.class);
        logger.info("Initialized PackageAccountReserveCache");
    }
    
    /**
     * Extract the primary key from a PackageAccountReserve entity
     */
    @Override
    protected Long extractKey(PackageAccountReserve entity) {
        return entity.getId();
    }
    
    /**
     * Find active reserve by package account ID
     * @param dbName Database name
     * @param packageAccountId Package account ID
     * @return Active reserve or null if not found
     */
    public PackageAccountReserve findActiveReserveByPackageAccountId(String dbName, Long packageAccountId) {
        Map<Long, PackageAccountReserve> allReserves = getAll(dbName);
        
        for (PackageAccountReserve reserve : allReserves.values()) {
            if (packageAccountId.equals(reserve.getPackageAccountId()) && 
                "RESERVED".equals(reserve.getStatus())) {
                return reserve;
            }
        }
        
        return null;
    }
    
    /**
     * Create a new reservation
     * @param dbName Database name
     * @param packageAccountId Package account ID
     * @param sessionId Session ID
     * @param amount Amount to reserve
     * @return The created reserve
     */
    public PackageAccountReserve createReservation(String dbName, Long packageAccountId, 
                                                  String sessionId, BigDecimal amount) {
        // Check for existing active reservation
        PackageAccountReserve existing = findActiveReserveByPackageAccountId(dbName, packageAccountId);
        if (existing != null) {
            logger.warn("Active reservation already exists for account {} in {}", packageAccountId, dbName);
            return existing;
        }
        
        // Create new reservation
        PackageAccountReserve reserve = new PackageAccountReserve();
        reserve.setId(System.currentTimeMillis()); // Generate ID based on timestamp
        reserve.setPackageAccountId(packageAccountId);
        reserve.setSessionId(sessionId);
        reserve.setReservedAmount(amount);
        reserve.setReservedAt(LocalDateTime.now());
        reserve.setStatus("RESERVED");
        reserve.setCurrentReserve(amount);
        
        // Add to cache
        put(dbName, reserve);
        
        logger.info("Created reservation {} for account {} in {}: amount={}", 
            reserve.getId(), packageAccountId, dbName, amount);
        
        return reserve;
    }
    
    /**
     * Release a reservation
     * @param dbName Database name
     * @param reserveId Reserve ID
     * @param releaseAmount Amount to release
     * @return true if successful, false if reserve not found
     */
    public boolean releaseReservation(String dbName, Long reserveId, BigDecimal releaseAmount) {
        PackageAccountReserve reserve = get(dbName, reserveId);
        if (reserve == null) {
            logger.warn("Reserve not found for release: db={}, id={}", dbName, reserveId);
            return false;
        }
        
        // Apply release delta (negative amount)
        reserve.applyDelta(releaseAmount.negate());
        
        // Update in cache
        update(dbName, reserveId, reserve);
        
        logger.info("Released {} from reservation {} in {}: currentReserve={}", 
            releaseAmount, reserveId, dbName, reserve.getCurrentReserve());
        
        return true;
    }
    
    /**
     * Get total reserved amount for a package account
     * @param dbName Database name
     * @param packageAccountId Package account ID
     * @return Total reserved amount
     */
    public BigDecimal getTotalReservedAmount(String dbName, Long packageAccountId) {
        BigDecimal total = BigDecimal.ZERO;
        Map<Long, PackageAccountReserve> allReserves = getAll(dbName);
        
        for (PackageAccountReserve reserve : allReserves.values()) {
            if (packageAccountId.equals(reserve.getPackageAccountId()) && 
                "RESERVED".equals(reserve.getStatus())) {
                total = total.add(reserve.getCurrentReserve());
            }
        }
        
        return total;
    }
    
    /**
     * Expire old reservations
     * @param dbName Database name
     * @param expirationTime Time after which reservations should be expired
     * @return Number of expired reservations
     */
    public int expireOldReservations(String dbName, LocalDateTime expirationTime) {
        Map<Long, PackageAccountReserve> allReserves = getAll(dbName);
        int expiredCount = 0;
        
        for (PackageAccountReserve reserve : allReserves.values()) {
            if ("RESERVED".equals(reserve.getStatus()) && 
                reserve.getReservedAt().isBefore(expirationTime)) {
                
                reserve.setStatus("EXPIRED");
                reserve.setReleasedAt(LocalDateTime.now());
                reserve.setCurrentReserve(BigDecimal.ZERO);
                
                update(dbName, reserve.getId(), reserve);
                expiredCount++;
                
                logger.info("Expired reservation {} in {}", reserve.getId(), dbName);
            }
        }
        
        return expiredCount;
    }
}