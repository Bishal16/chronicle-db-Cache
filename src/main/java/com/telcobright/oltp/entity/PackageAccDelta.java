package com.telcobright.oltp.entity;

import java.io.Serializable;
import java.math.BigDecimal;

public class PackageAccDelta implements Serializable {
    public final String dbName;
    public final long accountId;
    public final BigDecimal amount;
    public PackageAccDelta(String dbName, long accountId, BigDecimal amount) {
        this.dbName = dbName;
        this.accountId = accountId;
        this.amount = amount;
    }
}