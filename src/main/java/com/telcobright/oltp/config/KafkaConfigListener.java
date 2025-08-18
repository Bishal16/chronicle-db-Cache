package com.telcobright.oltp.config;

import com.telcobright.oltp.dbCache.CacheManager;
import com.telcobright.oltp.entity.PackageAccount;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

@ApplicationScoped
public class KafkaConfigListener {
    @Inject
    CacheManager cacheManager;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConfigListener.class);

    @Incoming("telcobright_all_tables")
    public void onMessage(DebeziumEvent event) {
        if (event.payload != null
                && event.payload.source != null
                && "packageaccount".equalsIgnoreCase(event.payload.source.table)) {

            String operation = event.payload.op;
            String dbName = event.payload.source.db;

            if ("c".equals(operation) && event.payload.after != null) {
                logger.info("🔥 New row inserted in cache, db: {} packageaccount: ID={}", dbName, event.payload.after.id);
                updateDbVsPkgIdVsPkgAccountCache(event.payload.after, dbName);
            } 
//            else if ("d".equals(operation) && event.payload.before != null) {
//                Long accountId = event.payload.before.id;
//                System.out.println("🗑️ Row deleted from packageaccount: ID=" + accountId);
//                packageAccountCache.delete(dbName, accountId);
//            }
//            else if ("u".equals(operation)) {
//                System.out.println("📝 Row updated in packageaccount: ID=" +
//                    (event.payload.after != null ? event.payload.after.id : event.payload.before.id));
//            }
        }
    }

    private void updateDbVsPkgIdVsPkgAccountCache(PackageAccountCdc after, String dbName) {
        PackageAccount newPackageAccount = new PackageAccount();
        newPackageAccount.setId(after.id);
        newPackageAccount.setPackagePurchaseId(after.packagePurchaseId);
        newPackageAccount.setName(after.name);

        newPackageAccount.setLastAmount(decodeBase64Decimal(after.lastAmount, 6));
        newPackageAccount.setBalanceBefore(decodeBase64Decimal(after.balanceBefore, 6));
        newPackageAccount.setBalanceAfter(decodeBase64Decimal(after.balanceAfter, 6));

        newPackageAccount.setUom(after.uom);
        newPackageAccount.setIsSelected(after.isSelected != null && after.isSelected == 1);

        cacheManager.updateAccountCache(dbName, newPackageAccount);
    }

    private BigDecimal decodeBase64Decimal(String base64Value, int scale) {
        if (base64Value == null) return null;
        byte[] bytes = Base64.getDecoder().decode(base64Value);
        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, scale);
    }
}
