package com.telcobright.oltp.cache;

import com.telcobright.core.cache.EntityCache;
import com.telcobright.oltp.entity.PackageAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for PackageAccount entities.
 * Structure: ConcurrentHashMap<dbname, ConcurrentHashMap<Long, PackageAccount>>
 */
public class PackageAccountCache extends EntityCache<Long, PackageAccount> {
    private static final Logger logger = LoggerFactory.getLogger(PackageAccountCache.class);
    
    /**
     * Create a new PackageAccount cache
     */
    public PackageAccountCache() {
        super(PackageAccount.class);
        logger.info("Initialized PackageAccountCache");
    }
    
    /**
     * Extract the primary key from a PackageAccount entity
     */
    @Override
    protected Long extractKey(PackageAccount entity) {
        return entity.getId();
    }
    
    /**
     * Update balance for an account
     * @param dbName Database name
     * @param accountId Account ID
     * @param deltaAmount Amount to subtract from balance
     * @return true if successful, false if account not found
     */
    public boolean updateBalance(String dbName, Long accountId, java.math.BigDecimal deltaAmount) {
        PackageAccount account = get(dbName, accountId);
        if (account == null) {
            logger.warn("Account not found for balance update: db={}, id={}", dbName, accountId);
            return false;
        }
        
        // Apply the delta
        account.applyDelta(deltaAmount);
        
        // Update in cache
        update(dbName, accountId, account);
        
        logger.debug("Updated balance for account {} in {}: delta={}, newBalance={}", 
            accountId, dbName, deltaAmount, account.getBalanceAfter());
        
        return true;
    }
    
    /**
     * Get available balance for an account
     * @param dbName Database name
     * @param accountId Account ID
     * @return Available balance or null if account not found
     */
    public java.math.BigDecimal getAvailableBalance(String dbName, Long accountId) {
        PackageAccount account = get(dbName, accountId);
        return account != null ? account.getBalanceAfter() : null;
    }
    
    /**
     * Check if account has sufficient balance
     * @param dbName Database name
     * @param accountId Account ID
     * @param requiredAmount Amount to check
     * @return true if sufficient balance, false otherwise
     */
    public boolean hasSufficientBalance(String dbName, Long accountId, java.math.BigDecimal requiredAmount) {
        java.math.BigDecimal balance = getAvailableBalance(dbName, accountId);
        return balance != null && balance.compareTo(requiredAmount) >= 0;
    }
}