package com.telcobright.oltp.entity;

import java.math.BigDecimal;

public interface DeltaOperation {
    String getDbName();
    Long getAccountId();
    BigDecimal getAmount();
}