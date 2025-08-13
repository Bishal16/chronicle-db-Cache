package com.telcobright.oltp.entity;

import java.math.BigDecimal;

public interface CacheableEntity {
    Long getId();
    void setId(Long id);
    void applyDelta(BigDecimal deltaAmount);
    String getTableName();
}