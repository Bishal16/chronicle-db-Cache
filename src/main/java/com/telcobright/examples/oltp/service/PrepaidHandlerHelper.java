package com.telcobright.examples.oltp.service;

import com.telcobright.examples.oltp.entity.PackageAccount;
import jakarta.enterprise.context.ApplicationScoped;
//import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

//@Service
@ApplicationScoped
public class PrepaidHandlerHelper {

    public List<Object[]> getPurchasedPackageAccountsByIdPartner(String database, Connection connection, int partnerId) {
        List<Object[]> resultList = new ArrayList<>();

        try {
            String sql = String.format("""
                 SELECT * FROM (
                    %s.packageaccount
                    Left JOIN %s.packagepurchase
                    ON packageaccount.id_PackagePurchase = packagepurchase.id)
                    
                   left join %s.packageitem
                    on packageitem.id_package = packagepurchase.id_package and packageitem.id_UOM = packageaccount.uom
                   where id_partner = ?
                      and status = 'ACTIVE'
                      and expireDate > Now()             
                   order by category, expireDate, priority desc
                   FOR UPDATE
                    """, database, database, database);

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                stmt.setInt(1, partnerId); // Set the parameter

                try (ResultSet resultSet = stmt.executeQuery()) {
                    int columnCount = resultSet.getMetaData().getColumnCount();

                    while (resultSet.next()) {
                        Object[] row = new Object[columnCount];
                        for (int i = 1; i <= columnCount; i++) {
                            row[i - 1] = resultSet.getObject(i);
                        }
                        resultList.add(row);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return resultList;
    }
    public Optional<PackageAccount> findPackageAccountById(String databaseName, Connection conn, long idPackageAccount) {
        String sql = String.format("""
            SELECT id_packageaccount, id_PackagePurchase, name, lastAmount,
                   balanceBefore, balanceAfter, uom
            FROM %s.packageaccount
            WHERE id_packageaccount = ? FOR UPDATE
            """, databaseName);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, idPackageAccount);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    PackageAccount pa = new PackageAccount();
//                    pa.seti(rs.getLong("id_packageaccount"));
                    pa.setPackagePurchaseId(rs.getLong("id_PackagePurchase"));
                    pa.setName(rs.getString("name"));
                    pa.setLastAmount(rs.getBigDecimal("lastAmount"));
                    pa.setBalanceBefore(rs.getBigDecimal("balanceBefore"));
                    pa.setBalanceAfter(rs.getBigDecimal("balanceAfter"));
                    pa.setUom(rs.getString("uom"));

                    return Optional.of(pa);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.empty();
    }
    public void updatePackageAccount(String databaseName,Connection conn,  PackageAccount updatedPackage) {
        String sql = String.format("""
            UPDATE %s.packageaccount
            SET lastAmount = ?, balanceBefore = ?, balanceAfter = ?
            WHERE id_packageaccount = ?
            """, databaseName);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setBigDecimal(1, updatedPackage.getLastAmount());
            stmt.setBigDecimal(2, updatedPackage.getBalanceBefore());
            stmt.setBigDecimal(3, updatedPackage.getBalanceAfter());
            stmt.setLong(4, updatedPackage.getId());

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to update package account", e);
        }
    }
//    public PackageAccountReserve getPackageAccountReserveByChannelCallUid(String databaseName,Connection conn,  String channelCallUid) {
//        String sql = String.format("""
//            SELECT id_packageaccountreserve, id_packageaccount, channel_call_uuid,
//                   id_PackagePurchase, name, reserveUnit, uom, time
//            FROM %s.packageaccountreserve
//            WHERE channel_call_uuid = ?
//            LIMIT 1
//            FOR UPDATE
//        """, databaseName);
//
//        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.setString(1, channelCallUid);
//            try (ResultSet rs = stmt.executeQuery()) {
//                if (rs.next()) {
//                    PackageAccountReserve reserve = new PackageAccountReserve();
//                    reserve.setIdPackageAccountReserve(rs.getLong("id_packageaccountreserve"));
//                    reserve.setIdPackageAccount(rs.getLong("id_packageaccount"));
//                    reserve.setChannel_call_uuid(rs.getString("channel_call_uuid"));
//                    reserve.setIdPackagePurchase(rs.getLong("id_PackagePurchase"));
//                    reserve.setName(rs.getString("name"));
//                    reserve.setReserveUnit(rs.getBigDecimal("reserveUnit"));
//                    reserve.setUom(rs.getString("uom"));
//                    reserve.setTime(rs.getString("time"));
//                    return reserve;
//                }
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException("Error fetching PackageAccountReserve by channel_call_uuid", e);
//        }
//
//        return null;
//    }
//    public void addNewPackageAccountReserve(String databaseName, Connection conn, PackageAccountReserve reserve) {
//        String sql = String.format("""
//            INSERT INTO %s.packageaccountreserve (
//                id_packageaccount, channel_call_uuid, id_PackagePurchase,
//                name, reserveUnit, uom, time
//            ) VALUES (?, ?, ?, ?, ?, ?, ?)
//        """, databaseName);
//
//        try (PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
//
//            stmt.setLong(1, reserve.getIdPackageAccount());
//            stmt.setString(2, reserve.getChannel_call_uuid());
//            stmt.setLong(3, reserve.getIdPackagePurchase());
//            stmt.setString(4, reserve.getName());
//            stmt.setBigDecimal(5, reserve.getReserveUnit());
//            stmt.setString(6, reserve.getUom());
//            stmt.setString(7, reserve.getTime());
//
//            int affectedRows = stmt.executeUpdate();
//
//            if (affectedRows == 0) {
//                throw new SQLException("Inserting packageaccountreserve failed, no rows affected.");
//            }
//
//            // Get generated ID (if needed)
//            try (ResultSet generatedKeys = stmt.getGeneratedKeys()) {
//                if (generatedKeys.next()) {
//                    reserve.setIdPackageAccountReserve(generatedKeys.getLong(1));
//                } else {
//                    throw new SQLException("Inserting packageaccountreserve failed, no ID obtained.");
//                }
//            }
//
//        } catch (SQLException e) {
//            throw new RuntimeException("Error saving PackageAccountReserve", e);
//        }
//    }
//    public void updatePackageAccountReserve(String databaseName, Connection conn, PackageAccountReserve reserve) {
//
//        String sql = String.format("""
//            UPDATE %s.packageaccountreserve
//            SET
//                id_packageaccount = ?,
//                channel_call_uuid = ?,
//                id_PackagePurchase = ?,
//                name = ?,
//                reserveUnit = ?,
//                uom = ?,
//                time = ?
//            WHERE id_packageaccountreserve = ?
//        """, databaseName);
//
//        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
//
//            stmt.setLong(1, reserve.getIdPackageAccount());
//            stmt.setString(2, reserve.getChannel_call_uuid());
//            stmt.setLong(3, reserve.getIdPackagePurchase());
//            stmt.setString(4, reserve.getName());
//            stmt.setBigDecimal(5, reserve.getReserveUnit());
//            stmt.setString(6, reserve.getUom());
//            stmt.setString(7, reserve.getTime());
//            stmt.setLong(8, reserve.getIdPackageAccountReserve());
//
//            int affectedRows = stmt.executeUpdate();
//
//            if (affectedRows == 0) {
//                throw new SQLException("Update failed: no rows affected.");
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }

    public void deletePackageAccountReserveByChannelCallUuid(String databaseName, Connection conn, String channelCallUid) {
        String sql = String.format("""
        DELETE FROM %s.packageaccountreserve
        WHERE channel_call_uuid = ?
    """, databaseName);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, channelCallUid);
            int rowsDeleted = stmt.executeUpdate();
            //System.out.println("Deleted package reserve for uuid: " + channelCallUid);
        } catch (SQLException e) {
            throw new RuntimeException("Error deleting package account reserve from DB: " + databaseName, e);
        }
    }

}

