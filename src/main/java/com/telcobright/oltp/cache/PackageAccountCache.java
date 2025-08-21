package com.telcobright.oltp.cache;

import com.telcobright.core.cache.EntityCache;
import com.telcobright.oltp.entity.PackageAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * Helper method to prepare accounts with balance updates for batch processing
     * @param updates Map of dbName -> Map of accountId -> deltaAmount
     * @return Map of dbName -> List of updated PackageAccount entities
     */
    public Map<String, List<PackageAccount>> prepareBalanceUpdates(Map<String, Map<Long, java.math.BigDecimal>> updates) {
        Map<String, List<PackageAccount>> preparedUpdates = new HashMap<>();
        
        for (Map.Entry<String, Map<Long, java.math.BigDecimal>> dbEntry : updates.entrySet()) {
            String dbName = dbEntry.getKey();
            List<PackageAccount> accountList = new ArrayList<>();
            
            for (Map.Entry<Long, java.math.BigDecimal> accountEntry : dbEntry.getValue().entrySet()) {
                Long accountId = accountEntry.getKey();
                java.math.BigDecimal deltaAmount = accountEntry.getValue();
                
                PackageAccount account = get(dbName, accountId);
                if (account == null) {
                    logger.warn("Account not found for batch update: db={}, id={}", dbName, accountId);
                    continue;
                }
                
                // Apply the delta
                account.applyDelta(deltaAmount);
                accountList.add(account);
            }
            
            if (!accountList.isEmpty()) {
                preparedUpdates.put(dbName, accountList);
            }
        }
        
        return preparedUpdates;
    }

}