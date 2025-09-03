package com.telcobright.examples.oltp.handlers;

import com.telcobright.core.cache.universal.GenericEntityHandler;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.examples.oltp.entity.PackageAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Entity handler for PackageAccount table.
 * Manages caching and database operations for PackageAccount entities.
 */
public class PackageAccountHandler extends GenericEntityHandler<Long, PackageAccount> {
    private static final Logger logger = LoggerFactory.getLogger(PackageAccountHandler.class);
    
    public PackageAccountHandler() {
        super("packageAccount", PackageAccount.class, "id");
    }
    
    @Override
    protected Long extractKey(WALEntry entry) {
        Object id = entry.get("id");
        if (id instanceof Long) {
            return (Long) id;
        } else if (id instanceof Number) {
            return ((Number) id).longValue();
        }
        return null;
    }
    
    @Override
    protected PackageAccount convertToEntity(WALEntry entry) {
        try {
            PackageAccount account = new PackageAccount();
            
            // Extract fields from WAL entry data
            Object id = entry.get("id");
            if (id != null) {
                account.setId(id instanceof Long ? (Long) id : Long.valueOf(id.toString()));
            }
            
            Object packagePurchaseId = entry.get("packagePurchaseId");
            if (packagePurchaseId != null) {
                account.setPackagePurchaseId(packagePurchaseId instanceof Long ? 
                    (Long) packagePurchaseId : Long.valueOf(packagePurchaseId.toString()));
            }
            
            Object name = entry.get("name");
            if (name != null) {
                account.setName(name.toString());
            }
            
            Object lastAmount = entry.get("lastAmount");
            if (lastAmount != null) {
                account.setLastAmount(lastAmount instanceof BigDecimal ? 
                    (BigDecimal) lastAmount : new BigDecimal(lastAmount.toString()));
            }
            
            Object balanceBefore = entry.get("balanceBefore");
            if (balanceBefore != null) {
                account.setBalanceBefore(balanceBefore instanceof BigDecimal ? 
                    (BigDecimal) balanceBefore : new BigDecimal(balanceBefore.toString()));
            }
            
            Object balanceAfter = entry.get("balanceAfter");
            if (balanceAfter != null) {
                account.setBalanceAfter(balanceAfter instanceof BigDecimal ? 
                    (BigDecimal) balanceAfter : new BigDecimal(balanceAfter.toString()));
            }
            
            Object uom = entry.get("uom");
            if (uom != null) {
                account.setUom(uom.toString());
            }
            
            Object isSelected = entry.get("isSelected");
            if (isSelected != null) {
                account.setIsSelected(isSelected instanceof Boolean ? 
                    (Boolean) isSelected : Boolean.valueOf(isSelected.toString()));
            }
            
            return account;
            
        } catch (Exception e) {
            logger.error("Failed to convert WAL entry to PackageAccount: {}", entry, e);
            return null;
        }
    }
    
    /**
     * Apply a balance delta to an account
     */
    public void applyBalanceDelta(String dbName, Long accountId, BigDecimal delta) {
        PackageAccount account = cache.get(dbName).get(accountId);
        if (account != null) {
            BigDecimal currentBalance = account.getBalanceAfter();
            BigDecimal newBalance = currentBalance.add(delta);
            account.setBalanceBefore(currentBalance);
            account.setBalanceAfter(newBalance);
            account.setLastAmount(delta);
            logger.debug("Applied balance delta: accountId={}, delta={}, newBalance={}", 
                       accountId, delta, newBalance);
        } else {
            logger.warn("Account not found in cache: dbName={}, accountId={}", dbName, accountId);
        }
    }
    
    @Override
    public void initialize() {
        super.initialize();
        logger.info("PackageAccountHandler initialized for table: {}", tableName);
        // Could load initial data from database here if needed
    }
}