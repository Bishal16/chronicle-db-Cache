package com.telcobright.oltp.dbCache;


import com.telcobright.oltp.entity.PackageAccDelta;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.zaxxer.hikari.HikariDataSource;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import net.openhft.chronicle.queue.ExcerptAppender;
//import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

@Startup
@ApplicationScoped
public class PackageAccountCache extends JdbcCache<Long, PackageAccount, List<PackageAccDelta>> {

    @Inject
    HikariDataSource dataSource;

    @Inject
    ChronicleInstance chronicleInstance;


//    @Inject
//    ExcerptAppender appender;

    public PackageAccountCache() {
        super(); // Calls no-args constructor
    }

    @PostConstruct
    void init() {
        if (dataSource == null) {
            throw new IllegalStateException("HikariDataSource not injected");
        }
        super.setDataSource(dataSource);

        try {
            initFromDb();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void initFromDb() throws SQLException {
        try (Connection conn = getConnection()) {
            String sql = """
            SELECT id_packageaccount AS id, id_PackagePurchase AS packagePurchaseId,
                   name, lastAmount, balanceBefore, balanceAfter, uom, isSelected
            FROM packageaccount
        """;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        PackageAccount acc = new PackageAccount();
//                        acc.setId(rs.getLong("id"));  // uncommented
                        acc.setPackagePurchaseId(rs.getLong("packagePurchaseId"));
                        acc.setName(rs.getString("name"));
                        acc.setLastAmount(rs.getBigDecimal("lastAmount"));
                        acc.setBalanceBefore(rs.getBigDecimal("balanceBefore"));
                        acc.setBalanceAfter(rs.getBigDecimal("balanceAfter"));
                        acc.setUom(rs.getString("uom"));
                        acc.setIsSelected(rs.getBoolean("isSelected"));

                        pkgIdVsPkgAccountCache.put(rs.getLong("id"), acc);
                    }
                    System.err.println("cache initialized successfully. account count: " + pkgIdVsPkgAccountCache.size());
                }
                catch (Exception e) {
                    System.err.println("couldn't initialize cache from db");
                    throw new RuntimeException(e);
                }
            }
        }
    }

//    protected void writeWALForUpdate_old(List<PackageAccDelta> entries) {
//        appender.writeDocument(w -> {
//            w.write("action").int32(CrudActionType.Update.ordinal());  // Write enum as int
//            w.write("size").int32(entries.size());
//            for (PackageAccDelta entry : entries) {
//                w.write("dbName").text(entry.dbName);
//                w.write("accountId").int32(entry.accountId);
//                w.write("amount").text(entry.amount.toPlainString());
//            }
//        });
//        long lastIndex = appender.lastIndexAppended();
//        System.out.println("Wrote WAL entry at index: " + lastIndex);
//
//    }
    @Override
    protected void writeWALForUpdate(List<PackageAccDelta> entries) {
    ExcerptAppender appender = chronicleInstance.getAppender();
    appender.writeDocument(w -> {
        w.write("action").int32(CrudActionType.Update.ordinal());  // Keep as int32
        w.write("size").int32(entries.size());
        for (PackageAccDelta entry : entries) {
            w.write("dbName").text(entry.dbName);
            w.write("accountId").int64(entry.accountId);  // 🔄 Fix this line
            w.write("amount").text(entry.amount.toPlainString());
        }
    });
    long lastIndex = appender.lastIndexAppended();
    System.out.println("Wrote WAL entry at index: " + lastIndex);
}

    @Override
    protected Consumer<List<PackageAccDelta>> getUpdateAction() {
        return packageAccDeltas -> {
            for (PackageAccDelta delta : packageAccDeltas) {
                PackageAccount targetAcc= pkgIdVsPkgAccountCache.get(delta.accountId);//super.entities
                if(targetAcc==null){
                   throw new RuntimeException("Package account [id: ]" +delta.accountId +
                           " not found in cache");
                }
                targetAcc.setLastAmount(delta.amount);
                targetAcc.setBalanceAfter(targetAcc.getBalanceAfter().add(delta.amount));
                targetAcc.setBalanceBefore(targetAcc.getBalanceAfter());
            }
        };
    }

    @Override
    protected void writeWALForInsert(PackageAccount newEntity) {
        ExcerptAppender appender = chronicleInstance.getAppender();
        appender.writeDocument(w -> {
            w.write("action").int32(CrudActionType.Insert.ordinal());
            w.write("packagePurchaseId").int64(newEntity.getPackagePurchaseId());
            w.write("name").text(newEntity.getName());
            w.write("lastAmount").text(newEntity.getLastAmount().toPlainString());
            w.write("balanceBefore").text(newEntity.getBalanceBefore().toPlainString());
            w.write("balanceAfter").text(newEntity.getBalanceAfter().toPlainString());
            w.write("uom").text(newEntity.getUom());
            w.write("isSelected").bool(Boolean.TRUE.equals(newEntity.getIsSelected()));
        });
    }


    @Override
    protected Consumer<PackageAccount> getInsertAction() {
        return newEntity -> {
            try {
                pkgIdVsPkgAccountCache.put(newEntity.getId(),newEntity);//super.entities
            } catch (Exception e) {
                throw new RuntimeException("Duplicate Entity, packageAccount [id: ]" +
                        newEntity.getId() + " already exists in the cache.");
            }
        };
    }
}