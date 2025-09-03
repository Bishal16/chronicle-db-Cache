package com.telcobright.core.cache.api;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Query request for cache read operations.
 * Supports various query patterns.
 */
public class CacheQuery implements Serializable {
    
    public enum QueryType {
        GET_BY_KEY,
        GET_BATCH,
        GET_ALL,
        FILTER,
        COUNT,
        EXISTS
    }
    
    private final QueryType type;
    private final String database;
    private final String table;
    private final Object key;
    private final Object[] keys;
    private final Map<String, Object> filterCriteria;
    private final int limit;
    private final int offset;
    
    private CacheQuery(Builder builder) {
        this.type = builder.type;
        this.database = builder.database;
        this.table = builder.table;
        this.key = builder.key;
        this.keys = builder.keys;
        this.filterCriteria = builder.filterCriteria;
        this.limit = builder.limit;
        this.offset = builder.offset;
    }
    
    // Factory methods for common queries
    
    public static CacheQuery getByKey(String database, String table, Object key) {
        return builder()
            .type(QueryType.GET_BY_KEY)
            .database(database)
            .table(table)
            .key(key)
            .build();
    }
    
    public static CacheQuery getBatch(String database, String table, Object... keys) {
        return builder()
            .type(QueryType.GET_BATCH)
            .database(database)
            .table(table)
            .keys(keys)
            .build();
    }
    
    public static CacheQuery getAll(String database, String table) {
        return builder()
            .type(QueryType.GET_ALL)
            .database(database)
            .table(table)
            .build();
    }
    
    public static CacheQuery filter(String database, String table, Map<String, Object> criteria) {
        return builder()
            .type(QueryType.FILTER)
            .database(database)
            .table(table)
            .filterCriteria(criteria)
            .build();
    }
    
    public static CacheQuery exists(String database, String table, Object key) {
        return builder()
            .type(QueryType.EXISTS)
            .database(database)
            .table(table)
            .key(key)
            .build();
    }
    
    public static CacheQuery count(String database, String table) {
        return builder()
            .type(QueryType.COUNT)
            .database(database)
            .table(table)
            .build();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
    public QueryType getType() { return type; }
    public String getDatabase() { return database; }
    public String getTable() { return table; }
    public Object getKey() { return key; }
    public Object[] getKeys() { return keys; }
    public Map<String, Object> getFilterCriteria() { return filterCriteria; }
    public int getLimit() { return limit; }
    public int getOffset() { return offset; }
    
    // Builder
    public static class Builder {
        private QueryType type;
        private String database;
        private String table;
        private Object key;
        private Object[] keys;
        private Map<String, Object> filterCriteria = new HashMap<>();
        private int limit = -1;
        private int offset = 0;
        
        public Builder type(QueryType type) {
            this.type = type;
            return this;
        }
        
        public Builder database(String database) {
            this.database = database;
            return this;
        }
        
        public Builder table(String table) {
            this.table = table;
            return this;
        }
        
        public Builder key(Object key) {
            this.key = key;
            return this;
        }
        
        public Builder keys(Object... keys) {
            this.keys = keys;
            return this;
        }
        
        public Builder filterCriteria(Map<String, Object> filterCriteria) {
            this.filterCriteria = filterCriteria;
            return this;
        }
        
        public Builder addFilter(String field, Object value) {
            this.filterCriteria.put(field, value);
            return this;
        }
        
        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }
        
        public Builder offset(int offset) {
            this.offset = offset;
            return this;
        }
        
        public CacheQuery build() {
            return new CacheQuery(this);
        }
    }
}