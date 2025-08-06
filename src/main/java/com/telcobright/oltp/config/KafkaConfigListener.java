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
                && event.payload.after != null
                && "c".equals(event.payload.op) // only inserts
                && event.payload.source != null
                && "packageaccount".equalsIgnoreCase(event.payload.source.table)) {

            System.out.println("🔥 New row inserted in packageaccount: ID=" + event.payload.after.id);

            updateDbVsPkgIdVsPkgAccountCache(event.payload.after, event.payload.source.db);
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
