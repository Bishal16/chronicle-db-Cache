package com.telcobright.core.cache;

import java.math.BigDecimal;

/**
 * Core interface for delta operations that modify cached entities.
 * Part of the generic Chronicle Queue caching framework.
 */
public interface DeltaOperation {
    /**
     * Get the database name where this operation should be applied.
     */
    String getDbName();
    
    /**
     * Get the ID of the entity this delta applies to.
     */
    Long getAccountId();
    
    /**
     * Get the amount/value of this delta operation.
     */
    BigDecimal getAmount();
}