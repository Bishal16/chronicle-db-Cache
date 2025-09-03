package com.telcobright.examples.oltp.service;

import com.telcobright.core.cache.annotations.Column;
import com.telcobright.core.cache.annotations.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Pre-builds and caches SQL statements at startup to avoid runtime reflection overhead.
 * Used by UniversalConsumer to write to database.
 */
public class SqlStatementCache {
    private static final Logger logger = LoggerFactory.getLogger(SqlStatementCache.class);
    
    // Cache of pre-built SQL statements per entity class
    private static final Map<Class<?>, EntitySqlStatements> cache = new ConcurrentHashMap<>();
    
    /**
     * Pre-built SQL statements for an entity
     */
    public static class EntitySqlStatements {
        public final String tableName;
        public final String primaryKeyColumn;
        public final String insertSql;
        public final String updateSql;
        public final String deleteSql;
        public final List<String> columnNames;
        public final Map<String, Field> columnFieldMap;
        
        private EntitySqlStatements(String tableName, String primaryKeyColumn, 
                                   String insertSql, String updateSql, String deleteSql,
                                   List<String> columnNames, Map<String, Field> columnFieldMap) {
            this.tableName = tableName;
            this.primaryKeyColumn = primaryKeyColumn;
            this.insertSql = insertSql;
            this.updateSql = updateSql;
            this.deleteSql = deleteSql;
            this.columnNames = columnNames;
            this.columnFieldMap = columnFieldMap;
        }
    }
    
    /**
     * Initialize SQL statements for an entity class at startup
     */
    public static void initialize(Class<?> entityClass) {
        if (cache.containsKey(entityClass)) {
            return; // Already initialized
        }
        
        // Get table annotation
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() + " must have @Table annotation");
        }
        String tableName = tableAnnotation.name();
        
        // Build column mapping
        Map<String, Field> columnFieldMap = new LinkedHashMap<>();
        List<String> columnNames = new ArrayList<>();
        String primaryKeyColumn = null;
        
        for (Field field : entityClass.getDeclaredFields()) {
            Column columnAnnotation = field.getAnnotation(Column.class);
            if (columnAnnotation != null) {
                field.setAccessible(true);
                String columnName = columnAnnotation.name();
                columnFieldMap.put(columnName, field);
                columnNames.add(columnName);
                
                if (columnAnnotation.primaryKey()) {
                    if (primaryKeyColumn != null) {
                        throw new IllegalArgumentException("Entity " + entityClass.getName() + " has multiple primary keys");
                    }
                    primaryKeyColumn = columnName;
                }
            }
        }
        
        if (primaryKeyColumn == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() + " must have a primary key");
        }
        
        // Build SQL statements (without database prefix - will be added at runtime)
        String insertSql = buildInsertSql(tableName, columnNames);
        String updateSql = buildUpdateSql(tableName, columnNames, primaryKeyColumn);
        String deleteSql = buildDeleteSql(tableName, primaryKeyColumn);
        
        EntitySqlStatements statements = new EntitySqlStatements(
            tableName, primaryKeyColumn, insertSql, updateSql, deleteSql, 
            columnNames, columnFieldMap
        );
        
        cache.put(entityClass, statements);
        
        logger.info("Pre-built SQL for {}: table={}, pk={}, columns={}", 
            entityClass.getSimpleName(), tableName, primaryKeyColumn, columnNames.size());
    }
    
    /**
     * Get pre-built SQL statements for an entity class
     */
    public static EntitySqlStatements get(Class<?> entityClass) {
        EntitySqlStatements statements = cache.get(entityClass);
        if (statements == null) {
            // Initialize on-demand if not already done
            initialize(entityClass);
            statements = cache.get(entityClass);
        }
        return statements;
    }
    
    /**
     * Get INSERT SQL with database name
     */
    public static String getInsertSql(Class<?> entityClass, String dbName) {
        EntitySqlStatements statements = get(entityClass);
        return String.format(statements.insertSql, dbName, statements.tableName);
    }
    
    /**
     * Get UPDATE SQL with database name
     */
    public static String getUpdateSql(Class<?> entityClass, String dbName) {
        EntitySqlStatements statements = get(entityClass);
        return String.format(statements.updateSql, dbName, statements.tableName, statements.primaryKeyColumn);
    }
    
    /**
     * Get DELETE SQL with database name
     */
    public static String getDeleteSql(Class<?> entityClass, String dbName) {
        EntitySqlStatements statements = get(entityClass);
        return String.format(statements.deleteSql, dbName, statements.tableName, statements.primaryKeyColumn);
    }
    
    private static String buildInsertSql(String tableName, List<String> columnNames) {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) {
                columns.append(", ");
                placeholders.append(", ");
            }
            columns.append(columnNames.get(i));
            placeholders.append("?");
        }
        
        // %s will be replaced with dbName at runtime
        return "INSERT INTO %s.%s (" + columns + ") VALUES (" + placeholders + ")";
    }
    
    private static String buildUpdateSql(String tableName, List<String> columnNames, String primaryKeyColumn) {
        StringBuilder setClause = new StringBuilder();
        
        boolean first = true;
        for (String column : columnNames) {
            if (!column.equals(primaryKeyColumn)) {
                if (!first) {
                    setClause.append(", ");
                }
                setClause.append(column).append(" = ?");
                first = false;
            }
        }
        
        // %s will be replaced with dbName at runtime
        return "UPDATE %s.%s SET " + setClause + " WHERE %s = ?";
    }
    
    private static String buildDeleteSql(String tableName, String primaryKeyColumn) {
        // %s will be replaced with dbName at runtime
        return "DELETE FROM %s.%s WHERE %s = ?";
    }
    
    /**
     * Initialize SQL statements for multiple entity classes at once
     */
    public static void initializeAll(Class<?>... entityClasses) {
        for (Class<?> entityClass : entityClasses) {
            initialize(entityClass);
        }
        logger.info("Pre-built SQL statements for {} entity classes", entityClasses.length);
    }
}