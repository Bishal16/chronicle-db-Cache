package com.telcobright.oltp.service;

import com.telcobright.oltp.dbCache.CrudActionType;
import com.telcobright.oltp.dbCache.CacheManager;
import com.telcobright.oltp.entity.PackageAccount;
import com.telcobright.oltp.queue.chronicle.ConsumerToQueue;
import com.zaxxer.hikari.HikariDataSource;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Wire;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class UniversalConsumer implements Runnable, ConsumerToQueue<Object> {
    private final ChronicleQueue queue;
    private final String consumerName;
    private final HikariDataSource dataSource;
    private final String offsetTable;
    private final boolean replayOnStart;
    private final ExcerptTailer tailer;
    private final ExcerptAppender appender;
    private final PendingStatusChecker pendingStatusChecker;
    private final CacheManager cacheManager;

    private volatile boolean running = true;

    private static final Logger logger = LoggerFactory.getLogger(UniversalConsumer.class);
    public UniversalConsumer(ChronicleQueue queue, ExcerptAppender appender, String consumerName,
                           HikariDataSource dataSource, String offsetTable, boolean replayOnStart, PendingStatusChecker pendingStatusChecker, CacheManager cacheManager) {

        this.queue = queue;
        this.appender = appender;
        this.consumerName = consumerName;
        this.dataSource = dataSource;
        this.offsetTable = offsetTable;
        this.replayOnStart = replayOnStart;
        this.pendingStatusChecker = pendingStatusChecker;
        this.cacheManager = cacheManager;
        this.tailer = queue.createTailer(consumerName);

        logger.info("✅ Initialized universal consumer '{}' for queue: {}", consumerName, queue);

        setTailerToLastProcessedOffset();

        //pendingStatusChecker.markReplayInProgress();
        try {
            if (replayOnStart && hasPendingMessages()) {
                logger.info("▶️ Starting pending msg replay with consumer {}", consumerName);
                replayPendingMessages();
                logger.info("▶️ Done with replay pending msg {}", consumerName);
            }
        }
        catch (Exception e) {
            logger.error("Error checking pending messages: ", e);
            throw new RuntimeException(e);
        }
        pendingStatusChecker.markReplayComplete();
    }

    private void setTailerToLastProcessedOffset() {
        try (Connection conn = dataSource.getConnection()) {
            String dbName = loadDbNameFromProperties();
            try (PreparedStatement createStmt = conn.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS " + dbName + "." + offsetTable + " (" +
                            "consumer_id VARCHAR(255) PRIMARY KEY, " +
                            "last_offset BIGINT NOT NULL" +
                            ")")) {
                createStmt.executeUpdate();
            }

            try {
                long lastSavedOffset = getLastOffsetFromDb(conn);
                logger.info("last saved offset in db for consumer: {}, is: {}", consumerName, lastSavedOffset);
                tailer.moveToIndex(lastSavedOffset + 1);
            } catch (SQLException e) {
                logger.info("✅ No previous offset for consumer '{}'. Starting fresh.", consumerName);
            }

        } catch (SQLException e) {
            logger.error("❌ Failed to load offset for consumer '{}': {}", consumerName, e.getMessage(), e);
            throw new RuntimeException("Offset load failed", e);
        }
    }

    @Override
    public void run() {
        logger.info("▶️ Consumer '{}' started", consumerName);
        processLiveMessages();
    }

    private boolean hasPendingMessagesOld() throws SQLException {
        long dbOffset;
        try (Connection conn = dataSource.getConnection()) {
            dbOffset = getLastOffsetFromDb(conn);
        }
        long lastQueueIndex;
        try{
            lastQueueIndex = appender.lastIndexAppended();      // in the fresh queue file, there is nothing
        }
        catch (Exception e){
            logger.info("🟢 Queue is empty: no messages have been appended yet. Exception: ");
            return false;
        }
        return lastQueueIndex > dbOffset;
    }
    private boolean hasPendingMessages() throws SQLException {
        long dbOffset;
        try (Connection conn = dataSource.getConnection()) {
            dbOffset = getLastOffsetFromDb(conn);
        }
        try {
            ExcerptTailer tailer = queue.createTailer("prepaid-consumer");
            long nextQueueIndex = tailer.index();

            logger.info("📌 DB offset: {}, Next queue index from tailer: {}", dbOffset, nextQueueIndex);

            return nextQueueIndex > dbOffset;
        } catch (Exception e) {
            logger.error("❌ Error while checking tailer index", e);
            return false;
        }
    }

    private void replayPendingMessages() {
        while (running) {
            try (Connection conn = dataSource.getConnection()) {
                conn.setAutoCommit(false);

                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent())
                        break;

                    processMessage(dc, conn);

                    conn.commit();
                } catch (Exception e) {
                    conn.rollback();
                    throw e;
                }
            } catch (Exception e) {
                logger.error("❌ Error during replay for consumer '{}': {}", consumerName, e.getMessage(), e);
            }
        }
    }

    private void processLiveMessages() {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            while (running) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (dc.isPresent()) {
                        try {
                            processMessage(dc, conn);
                            conn.commit();
                        } catch (Exception e) {
                            conn.rollback();
                            logger.error("❌ Error processing message: ", e);
                        }
                    } else {
                        Thread.sleep(100);
                    }
                }
            }
        } catch (SQLException | InterruptedException e) {
            logger.error("❌ DB connection error in live message processing", e);
        }
    }
//    private void processLiveMessages() {
//        while (running) {
//            try (Connection conn = dataSource.getConnection()) {
//                conn.setAutoCommit(false);
//
//                try (DocumentContext dc = tailer.readingDocument()) {
//                    if (dc.isPresent()) {
//                        processMessage(dc, conn);
//                        conn.commit();
//                    } else {
//                        Thread.sleep(100);
//                    }
//                } catch (Exception e) {
//                    conn.rollback();
//                    logger.error("❌ Error while processing live messages for consumer '{}': {}", consumerName, e.getMessage(), e);
//                }
//            } catch (SQLException e) {
//                logger.error("❌ Error obtaining DB connection or interrupted for consumer '{}': {}", consumerName, e.getMessage(), e);
//            }
//        }
//    }

    private void processMessage(DocumentContext dc, Connection conn) throws SQLException {
        Wire wire = dc.wire();
        
        // Read entity type (optional for backward compatibility)
        String entity = wire.read(() -> "entity").text();
        
        // Read table name - this is now the primary identifier
        String tableName = wire.read(() -> "tableName").text();
        if (tableName == null) {
            // Legacy format - default to packageaccount
            tableName = "packageaccount";
        }
        
        int actionOrdinal = wire.read("action").int32();
        CrudActionType actionType = CrudActionType.values()[actionOrdinal];
        logger.info("Processing action={} for table={}", actionType, tableName);

        if(actionType.equals(CrudActionType.Update)){
            int size = wire.read("size").int32();
            logger.info("Processing {} deltas for update on table {}", size, tableName);
            
            for (int i = 0; i < size; i++) {
                String dbName = wire.read("dbName").text();
                
                if ("packageaccount".equals(tableName)) {
                    long accountId = wire.read("accountId").int64();
                    String amountStr = wire.read("amount").text();
                    BigDecimal amount = amountStr == null || amountStr.isEmpty() ? null : new BigDecimal(amountStr);
                    
                    updatePackageAccountTable(dbName, accountId, amount, conn, tableName);
                    persistProcessedDeltaLogs(conn, consumerName, tailer.index(), dbName, accountId, amount, tableName);
                    
                } else if ("packageaccountreserve".equals(tableName)) {
                    long reserveId = wire.read("reserveId").int64();
                    String amountStr = wire.read("amount").text();
                    String sessionId = wire.read("sessionId").text();
                    BigDecimal amount = amountStr == null || amountStr.isEmpty() ? null : new BigDecimal(amountStr);
                    
                    updatePackageAccountReserveTable(dbName, reserveId, amount, sessionId, conn, tableName);
                    persistReserveProcessedLogs(conn, consumerName, tailer.index(), dbName, reserveId, amount, sessionId, tableName);
                }
            }

            saveOffsetToDb(conn, tailer.index());

            logger.info("✅ Consumer '{}' processed update of full batch at offset: {}", consumerName, tailer.index());
        }
        else if(actionType.equals(CrudActionType.Delete)){
            String dbName = wire.read("dbName").text();
            // Read entityId (new format) or accountId (legacy format)
            Long entityId = wire.read(() -> "entityId").int64();
            long id = (entityId != null) ? entityId : wire.read("accountId").int64();
            
            deleteFromDb(dbName, id, conn, tableName);
            persistDeleteLog(conn, consumerName, tailer.index(), dbName, id, tableName);
            
            saveOffsetToDb(conn, tailer.index());
            
            logger.info("✅ Consumer '{}' processed delete for id={} in table={} db={} at offset: {}", 
                consumerName, id, tableName, dbName, tailer.index());
        }
//        else if(actionType.equals(CrudActionType.Insert)){
//            long idPackageAcc = wire.read("idPackageAccount").int64();
//
//            PackageAccount newPackageAccount = new PackageAccount();
//
//            newPackageAccount.setPackagePurchaseId(wire.read("packagePurchaseId").int64());
//            newPackageAccount.setName(wire.read("name").text());
//            newPackageAccount.setLastAmount(new BigDecimal(wire.read("lastAmount").text()));
//            newPackageAccount.setBalanceBefore(new BigDecimal(wire.read("balanceBefore").text()));
//            newPackageAccount.setBalanceAfter(new BigDecimal(wire.read("balanceAfter").text()));
//            newPackageAccount.setUom(wire.read("uom").text());
//            newPackageAccount.setIsSelected(Boolean.valueOf(wire.read("").text()));
//
//            packageAccountCache.getAccountCache().get().put(idPackageAcc, newPackageAccount);
//            logger.info("✅ Consumer '{}' processed insert of full batch at offset: {}", consumerName, tailer.index());
//        }

    }

    private void saveOffsetToDb(Connection conn, long offset) throws SQLException {
        String dbName = loadDbNameFromProperties();
        String sql = "INSERT INTO " + dbName + "." + offsetTable + " (consumer_id, last_offset) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE last_offset = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, consumerName);
            stmt.setLong(2, offset);
            stmt.setLong(3, offset);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("❌ Failed to save offset to db for consumer '{}': {}", consumerName, e.getMessage());
            throw new SQLException("Failed to save offset to db");
        }
    }

    @Override
    public void updatePackageAccountTable(Object delta, Connection conn) throws SQLException {
        // This method is for interface compatibility only
        throw new UnsupportedOperationException("Use table-specific update methods");
    }
    
    private void updatePackageAccountTable(String dbName, long accountId, BigDecimal amount, Connection conn, String tableName) throws SQLException {
        String sql = String.format(
            "UPDATE %s.%s SET lastAmount=-?, balanceBefore=balanceAfter, balanceAfter=balanceAfter-? WHERE id_packageaccount=?",
            dbName, tableName);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setBigDecimal(1, amount);
            stmt.setBigDecimal(2, amount);
            stmt.setLong(3, accountId);
            int updated = stmt.executeUpdate();
            if (updated == 0) {
                throw new SQLException("No " + tableName + " found for id=" + accountId);
            }
        }
    }

    public long getLastOffsetFromDb(Connection conn) throws SQLException {
        String dbName = loadDbNameFromProperties();
        String sql = "SELECT last_offset FROM " + dbName + "." + offsetTable + " WHERE consumer_id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, consumerName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("last_offset");
                } else {
                    throw new SQLException("Offset not found for consumer=" + consumerName);
                }
            }
        }
    }

    private void updatePackageAccountReserveTable(String dbName, long reserveId, BigDecimal amount, String sessionId, Connection conn, String tableName) throws SQLException {
        if (amount.signum() > 0) {
            // Positive amount - new reservation or addition to existing
            String sql = String.format("""
                INSERT INTO %s.%s 
                (id_packageaccountreserve, sessionId, reservedAmount, currentReserve, reservedAt, status)
                VALUES (?, ?, ?, ?, NOW(), 'RESERVED')
                ON DUPLICATE KEY UPDATE 
                    reservedAmount = reservedAmount + VALUES(reservedAmount),
                    currentReserve = currentReserve + VALUES(currentReserve)
            """, dbName, tableName);
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, reserveId);
                stmt.setString(2, sessionId);
                stmt.setBigDecimal(3, amount);
                stmt.setBigDecimal(4, amount);
                int affected = stmt.executeUpdate();
                
                if (affected > 0) {
                    logger.info("✅ Reserved amount {} for reserveId={} in db={}", amount, reserveId, dbName);
                }
            }
        } else if (amount.signum() < 0) {
            // Negative amount - release reservation
            BigDecimal releaseAmount = amount.abs();
            
            String sql = String.format("""
                UPDATE %s.%s 
                SET currentReserve = currentReserve - ?,
                    status = CASE 
                        WHEN currentReserve - ? <= 0 THEN 'RELEASED'
                        ELSE status
                    END,
                    releasedAt = CASE
                        WHEN currentReserve - ? <= 0 THEN NOW()
                        ELSE releasedAt
                    END
                WHERE id_packageaccountreserve = ? AND currentReserve >= ?
            """, dbName, tableName);
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setBigDecimal(1, releaseAmount);
                stmt.setBigDecimal(2, releaseAmount);
                stmt.setBigDecimal(3, releaseAmount);
                stmt.setLong(4, reserveId);
                stmt.setBigDecimal(5, releaseAmount);
                
                int updated = stmt.executeUpdate();
                if (updated == 0) {
                    throw new SQLException("No reserve found or insufficient reserve for id=" + reserveId);
                }
                
                logger.info("✅ Released amount {} from reserveId={} in db={}", releaseAmount, reserveId, dbName);
            }
        }
    }
    
    private void persistReserveProcessedLogs(Connection conn, String consumerName, long offset, String dbName, long reserveId, BigDecimal amount, String sessionId, String tableName) throws SQLException {
        String sql = String.format("""
            INSERT INTO %s.reserve_log (consumer_name, processed_at, offset, db_name, reserve_id, amount, session_id, action_type)
            VALUES (?, NOW(), ?, ?, ?, ?, ?, ?)
        """, dbName);
        
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, consumerName);
            ps.setLong(2, offset);
            ps.setString(3, dbName);
            ps.setLong(4, reserveId);
            if (amount != null) {
                ps.setBigDecimal(5, amount);
            } else {
                ps.setNull(5, java.sql.Types.DECIMAL);
            }
            ps.setString(6, sessionId);
            ps.setString(7, amount.signum() > 0 ? "RESERVE" : "RELEASE");
            ps.executeUpdate();
        }
    }
    
    private void deleteFromDb(String dbName, long id, Connection conn, String tableName) throws SQLException {
        // Determine ID column name based on table
        String idColumn = getIdColumnName(tableName);
        
        String sql = String.format("DELETE FROM %s.%s WHERE %s = ?", dbName, tableName, idColumn);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, id);
            int deleted = stmt.executeUpdate();
            if (deleted == 0) {
                logger.warn("⚠️ No record found to delete for id={} in {}.{}", id, dbName, tableName);
            } else {
                logger.info("✅ Deleted id={} from {}.{}", id, dbName, tableName);
            }
        }
    }
    
    private String getIdColumnName(String tableName) {
        switch (tableName.toLowerCase()) {
            case "packageaccount":
                return "id_packageaccount";
            case "packageaccountreserve":
                return "id_packageaccountreserve";
            default:
                // Generic fallback - assumes id column follows pattern id_<tablename>
                return "id_" + tableName.toLowerCase();
        }
    }

    private void persistDeleteLog(Connection conn, String consumerName, long offset, String dbName, long entityId, String tableName) throws SQLException {
        // Use appropriate log table based on entity type
        String logTable = tableName.equals("packageaccountreserve") ? "reserve_log" : "delta_log";
        String idColumn = tableName.equals("packageaccountreserve") ? "reserve_id" : "account_id";
        
        String sql = String.format("""
        INSERT INTO %s.%s (consumer_name, processed_at, offset, db_name, %s, action_type)
        VALUES (?, NOW(), ?, ?, ?, 'DELETE')
    """, dbName, logTable, idColumn);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, consumerName);
            ps.setLong(2, offset);
            ps.setString(3, dbName);
            ps.setLong(4, entityId);
            ps.executeUpdate();
        }
    }

    private void persistProcessedDeltaLogs(Connection conn, String consumerName, long offset, String dbName, long accountId, BigDecimal amount, String tableName) throws SQLException {
        // Sanitize or validate dbName if it comes from user input
        String sql = String.format("""
        INSERT INTO %s.delta_log (consumer_name, processed_at, offset, db_name, account_id, amount)
        VALUES (?, NOW(), ?, ?, ?, ?)
    """, dbName);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, consumerName);
            ps.setLong(2, offset);
            ps.setString(3, dbName); // still setting db_name as a column value
            ps.setLong(4, accountId);
            if (amount != null) {
                ps.setBigDecimal(5, amount);
            } else {
                ps.setNull(5, java.sql.Types.DECIMAL);
            }
            ps.executeUpdate();
        }
    }

    public void shutdown() {
        running = false;
        try {
            if (tailer != null) tailer.close();
            logger.info("✅ Consumer '{}' shutdown complete", consumerName);
        } catch (Exception e) {
            logger.info("❌ Consumer '{}' shutdown error: {}", consumerName, e.getMessage());
        }
    }
    private String loadDbNameFromProperties() {
        Properties properties = new Properties();
        try (InputStream input = UniversalConsumer.class.getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input == null) {
                throw new RuntimeException("❌ application.properties not found in classpath");
            }

            properties.load(input);
            return properties.getProperty("db.name");

        } catch (IOException e) {
            throw new RuntimeException("❌ Failed to load db.name from properties file", e);
        }
    }

}
