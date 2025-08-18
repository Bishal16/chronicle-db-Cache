package com.telcobright.oltp.entity;

import com.telcobright.core.cache.DeltaOperation;
import java.io.Serializable;
import java.math.BigDecimal;

public class PackageAccDelta implements Serializable, DeltaOperation {
    public final String dbName;
    public final long accountId;
    public final BigDecimal amount;
    
    public PackageAccDelta(String dbName, long accountId, BigDecimal amount) {
        this.dbName = dbName;
        this.accountId = accountId;
        this.amount = amount;
    }

    @Override
    public String getDbName() {
        return dbName;
    }

    @Override
    public Long getAccountId() {
        return accountId;
    }

    @Override
    public BigDecimal getAmount() {
        return amount;
    }
}