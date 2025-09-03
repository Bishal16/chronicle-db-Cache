package com.telcobright.core.cache.client.api;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Query filter for cache queries.
 * Supports both lambda predicates (for embedded) and serializable criteria (for remote).
 */
public interface QueryFilter<V> extends Serializable {
    
    /**
     * Test if a value matches the filter.
     * 
     * @param value Value to test
     * @return true if matches
     */
    boolean test(V value);
    
    /**
     * Get SQL-like WHERE clause for remote execution.
     * 
     * @return WHERE clause string
     */
    String toWhereClause();
    
    /**
     * Create a filter from a predicate (for embedded use).
     */
    static <V> QueryFilter<V> fromPredicate(Predicate<V> predicate) {
        return new QueryFilter<V>() {
            @Override
            public boolean test(V value) {
                return predicate.test(value);
            }
            
            @Override
            public String toWhereClause() {
                throw new UnsupportedOperationException("Lambda predicates cannot be converted to SQL");
            }
        };
    }
    
    /**
     * Create a filter from a WHERE clause (for remote use).
     */
    static <V> QueryFilter<V> fromWhereClause(String whereClause) {
        return new QueryFilter<V>() {
            @Override
            public boolean test(V value) {
                throw new UnsupportedOperationException("SQL WHERE clauses cannot be executed locally");
            }
            
            @Override
            public String toWhereClause() {
                return whereClause;
            }
        };
    }
}