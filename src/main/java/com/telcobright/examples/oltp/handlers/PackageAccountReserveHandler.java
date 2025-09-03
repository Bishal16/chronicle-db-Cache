package com.telcobright.examples.oltp.handlers;

import com.telcobright.core.cache.universal.GenericEntityHandler;
import com.telcobright.core.wal.WALEntry;
import com.telcobright.examples.oltp.entity.PackageAccountReserve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Entity handler for PackageAccountReserve table.
 * Manages caching and database operations for PackageAccountReserve entities.
 */
public class PackageAccountReserveHandler extends GenericEntityHandler<Long, PackageAccountReserve> {
    private static final Logger logger = LoggerFactory.getLogger(PackageAccountReserveHandler.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    public PackageAccountReserveHandler() {
        super("packageAccountReserve", PackageAccountReserve.class, "id");
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
    protected PackageAccountReserve convertToEntity(WALEntry entry) {
        try {
            PackageAccountReserve reserve = new PackageAccountReserve();
            
            // Extract fields from WAL entry data
            Object id = entry.get("id");
            if (id != null) {
                reserve.setId(id instanceof Long ? (Long) id : Long.valueOf(id.toString()));
            }
            
            Object packageAccountId = entry.get("packageAccountId");
            if (packageAccountId != null) {
                reserve.setPackageAccountId(packageAccountId instanceof Long ? 
                    (Long) packageAccountId : Long.valueOf(packageAccountId.toString()));
            }
            
            Object sessionId = entry.get("sessionId");
            if (sessionId != null) {
                reserve.setSessionId(sessionId.toString());
            }
            
            Object reservedAmount = entry.get("reservedAmount");
            if (reservedAmount != null) {
                reserve.setReservedAmount(reservedAmount instanceof BigDecimal ? 
                    (BigDecimal) reservedAmount : new BigDecimal(reservedAmount.toString()));
            }
            
            Object currentReserve = entry.get("currentReserve");
            if (currentReserve != null) {
                reserve.setCurrentReserve(currentReserve instanceof BigDecimal ? 
                    (BigDecimal) currentReserve : new BigDecimal(currentReserve.toString()));
            }
            
            Object status = entry.get("status");
            if (status != null) {
                reserve.setStatus(status.toString());
            }
            
            Object reservedAt = entry.get("reservedAt");
            if (reservedAt != null) {
                if (reservedAt instanceof LocalDateTime) {
                    reserve.setReservedAt((LocalDateTime) reservedAt);
                } else {
                    reserve.setReservedAt(LocalDateTime.parse(reservedAt.toString(), DATE_TIME_FORMATTER));
                }
            }
            
            Object releasedAt = entry.get("releasedAt");
            if (releasedAt != null) {
                if (releasedAt instanceof LocalDateTime) {
                    reserve.setReleasedAt((LocalDateTime) releasedAt);
                } else {
                    reserve.setReleasedAt(LocalDateTime.parse(releasedAt.toString(), DATE_TIME_FORMATTER));
                }
            }
            
            return reserve;
            
        } catch (Exception e) {
            logger.error("Failed to convert WAL entry to PackageAccountReserve: {}", entry, e);
            return null;
        }
    }
    
    /**
     * Apply a reserve delta to a reservation
     */
    public void applyReserveDelta(String dbName, Long reserveId, BigDecimal delta, String sessionId) {
        PackageAccountReserve reserve = cache.get(dbName).get(reserveId);
        if (reserve != null) {
            reserve.applyDelta(delta);
            if (sessionId != null && !sessionId.isEmpty()) {
                reserve.setSessionId(sessionId);
            }
            logger.debug("Applied reserve delta: reserveId={}, delta={}, newReserve={}", 
                       reserveId, delta, reserve.getCurrentReserve());
        } else {
            logger.warn("Reserve not found in cache: dbName={}, reserveId={}", dbName, reserveId);
        }
    }
    
    /**
     * Release a reservation
     */
    public void releaseReservation(String dbName, Long reserveId) {
        PackageAccountReserve reserve = cache.get(dbName).get(reserveId);
        if (reserve != null) {
            reserve.setStatus("RELEASED");
            reserve.setReleasedAt(LocalDateTime.now());
            reserve.setCurrentReserve(BigDecimal.ZERO);
            logger.debug("Released reservation: reserveId={}", reserveId);
        }
    }
    
    @Override
    public void initialize() {
        super.initialize();
        logger.info("PackageAccountReserveHandler initialized for table: {}", tableName);
        // Could load initial data from database here if needed
    }
}