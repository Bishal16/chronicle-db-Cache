package com.telcobright.examples.oltp.entity;

import com.telcobright.core.cache.DeltaOperation;
import java.io.Serializable;
import java.math.BigDecimal;

public class PackageAccountReserveDelta implements Serializable, DeltaOperation {
    public final String dbName;
    public final long reserveId;
    public final BigDecimal amount;
    public final String sessionId;
    
    public PackageAccountReserveDelta(String dbName, long reserveId, BigDecimal amount, String sessionId) {
        this.dbName = dbName;
        this.reserveId = reserveId;
        this.amount = amount;
        this.sessionId = sessionId;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Long getAccountId() {
        return reserveId;
    }

    @Override
    public BigDecimal getAmount() {
        return amount;
    }
    
    public String getSessionId() {
        return sessionId;
    }
}