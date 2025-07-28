package com.telcobright.oltp.service;

import com.telcobright.oltp.dbCache.CrudActionType;
import com.telcobright.oltp.entity.PackageAccDelta;
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

public class PrepaidConsumer implements Runnable, ConsumerToQueue<PackageAccDelta> {
    private final ChronicleQueue queue;
    private final String consumerName;
    private final HikariDataSource dataSource;
    private final String offsetTable;
    private final boolean replayOnStart;
    private final ExcerptTailer tailer;
    private final ExcerptAppender appender;
    private final PendingStatusChecker pendingStatusChecker;

    private volatile boolean running = true;

    private static final Logger logger = LoggerFactory.getLogger(PrepaidConsumer.class);
    public PrepaidConsumer( ChronicleQueue queue, ExcerptAppender appender, String consumerName,
            HikariDataSource dataSource, String offsetTable, boolean replayOnStart, PendingStatusChecker pendingStatusChecker) {

        this.queue = queue;
        this.appender = appender;
        this.consumerName = consumerName;
        this.dataSource = dataSource;
        this.offsetTable = offsetTable;
        this.replayOnStart = replayOnStart;
        this.pendingStatusChecker = pendingStatusChecker;
        this.tailer = queue.createTailer(consumerName);

        logger.info("✅ Initialized consumer '{}' for queue: {}", consumerName, queue);

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
        int actionOrdinal = wire.read("action").int32();
        CrudActionType actionType = CrudActionType.values()[actionOrdinal];
        int size = wire.read("size").int32();
        logger.info("Processing action={} with {} deltas", actionType, size);

        for (int i = 0; i < size; i++) {
            String dbName = wire.read("dbName").text();
            long accountId = wire.read("accountId").int64();
            String amountStr = wire.read("amount").text();

            BigDecimal amount = amountStr == null || amountStr.isEmpty() ? null : new BigDecimal(amountStr);
            PackageAccDelta delta = new PackageAccDelta(dbName, accountId, amount);

            updatePackageAccountTable(delta, conn);
            persistProcessedDeltaLogs(conn, consumerName, tailer.index(), dbName, accountId, amount);
        }

        saveOffsetToDb(conn, tailer.index());

        logger.info("✅ Consumer '{}' processed full batch at offset: {}", consumerName, tailer.index());
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
    public void updatePackageAccountTable(PackageAccDelta delta, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "UPDATE " + delta.dbName + ".packageaccount SET lastAmount=-?, balanceBefore=balanceAfter, balanceAfter=balanceAfter-? WHERE id_packageaccount=?")) {
            stmt.setBigDecimal(1, delta.amount);
            stmt.setBigDecimal(2, delta.amount);
            stmt.setLong(3, delta.accountId);
            int updated = stmt.executeUpdate();
            if (updated == 0) {
                throw new SQLException("No packageaccount found for id=" + delta.accountId);
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

    private void persistProcessedDeltaLogs(Connection conn, String consumerName, long offset, String dbName, long accountId, BigDecimal amount) throws SQLException {
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
        try (InputStream input = PrepaidConsumer.class.getClassLoader()
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
