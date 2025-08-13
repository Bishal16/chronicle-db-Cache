package com.telcobright.oltp.config;

import com.telcobright.oltp.dbCache.PackageAccountCache;
import com.telcobright.oltp.entity.PackageAccount;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

@ApplicationScoped
public class KafkaConfigListener {
    @Inject
    PackageAccountCache packageAccountCache;

    @Incoming("telcobright_all_tables")
    public void onMessage(DebeziumEvent event) {
        if (event.payload != null
                && event.payload.source != null
                && "packageaccount".equalsIgnoreCase(event.payload.source.table)) {

            String operation = event.payload.op;
            String dbName = event.payload.source.db;

            if ("c".equals(operation) && event.payload.after != null) {
                // Handle INSERT
                System.out.println("🔥 New row inserted in packageaccount: ID=" + event.payload.after.id);
                updateDbVsPkgIdVsPkgAccountCache(event.payload.after, dbName);
            } 
            else if ("d".equals(operation) && event.payload.before != null) {
                // Handle DELETE
                Long accountId = event.payload.before.id;
                System.out.println("🗑️ Row deleted from packageaccount: ID=" + accountId);
                packageAccountCache.delete(dbName, accountId);
            }
            else if ("u".equals(operation)) {
                // Handle UPDATE if needed in future
                System.out.println("📝 Row updated in packageaccount: ID=" + 
                    (event.payload.after != null ? event.payload.after.id : event.payload.before.id));
            }
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

        packageAccountCache.updateAccountCache(dbName, newPackageAccount);
    }

    private BigDecimal decodeBase64Decimal(String base64Value, int scale) {
        if (base64Value == null) return null;
        byte[] bytes = Base64.getDecoder().decode(base64Value);
        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, scale);
    }
}
