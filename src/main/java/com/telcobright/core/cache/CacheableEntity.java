package com.telcobright.core.cache;

import java.math.BigDecimal;

/**
 * Core interface for entities that can be cached.
 * Part of the generic Chronicle Queue caching framework.
 */
public interface CacheableEntity {
    /**
     * Get the unique identifier for this entity.
     */
    Long getId();
    
    /**
     * Set the unique identifier for this entity.
     */
    void setId(Long id);
    
    /**
     * Apply a delta operation to this entity.
     * @param deltaAmount The amount to apply (can be positive or negative)
     */
    void applyDelta(BigDecimal deltaAmount);
    
    /**
     * Get the database table name for this entity.
     */
    String getTableName();
}