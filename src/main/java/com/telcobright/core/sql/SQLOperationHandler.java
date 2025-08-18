package com.telcobright.core.sql;

import com.telcobright.core.wal.WALEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Handles SQL operations for WAL entries.
 * Generates and executes SQL statements based on WAL entry data.
 */
public class SQLOperationHandler {
    private static final Logger logger = LoggerFactory.getLogger(SQLOperationHandler.class);
    
    /**
     * Execute a WAL entry as a SQL operation.
     * @param conn The database connection
     * @param entry The WAL entry to execute
     * @return Number of affected rows
     */
    public int execute(Connection conn, WALEntry entry) throws SQLException {
        switch (entry.getOperationType()) {
            case INSERT:
                return executeInsert(conn, entry);
            case UPDATE:
                return executeUpdate(conn, entry);
            case DELETE:
                return executeDelete(conn, entry);
            case UPSERT:
                return executeUpsert(conn, entry);
            default:
                throw new UnsupportedOperationException("Operation type not supported: " + entry.getOperationType());
        }
    }
    
    private int executeInsert(Connection conn, WALEntry entry) throws SQLException {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        
        for (Map.Entry<String, Object> dataEntry : entry.getData().entrySet()) {
            // Skip internal fields
            if (!dataEntry.getKey().startsWith("_")) {
                columns.add(dataEntry.getKey());
                values.add(dataEntry.getValue());
            }
        }
        
        String sql = String.format("INSERT INTO %s.%s (%s) VALUES (%s)",
            entry.getDbName(),
            entry.getTableName(),
            String.join(", ", columns),
            columns.stream().map(c -> "?").collect(Collectors.joining(", "))
        );
        
        logger.debug("Executing INSERT: {}", sql);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                setParameter(stmt, i + 1, values.get(i));
            }
            return stmt.executeUpdate();
        }
    }
    
    private int executeUpdate(Connection conn, WALEntry entry) throws SQLException {
        // Separate WHERE conditions from SET values
        Map<String, Object> whereConditions = extractWhereConditions(entry);
        Map<String, Object> setValues = extractSetValues(entry);
        
        if (setValues.isEmpty()) {
            throw new SQLException("No values to update");
        }
        
        if (whereConditions.isEmpty()) {
            throw new SQLException("No WHERE conditions specified for UPDATE");
        }
        
        List<Object> parameters = new ArrayList<>();
        
        // Build SET clause
        String setClause = setValues.entrySet().stream()
            .map(e -> {
                parameters.add(e.getValue());
                return e.getKey() + " = ?";
            })
            .collect(Collectors.joining(", "));
        
        // Build WHERE clause
        String whereClause = whereConditions.entrySet().stream()
            .map(e -> {
                parameters.add(e.getValue());
                return e.getKey() + " = ?";
            })
            .collect(Collectors.joining(" AND "));
        
        String sql = String.format("UPDATE %s.%s SET %s WHERE %s",
            entry.getDbName(),
            entry.getTableName(),
            setClause,
            whereClause
        );
        
        logger.debug("Executing UPDATE: {}", sql);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                setParameter(stmt, i + 1, parameters.get(i));
            }
            return stmt.executeUpdate();
        }
    }
    
    private int executeDelete(Connection conn, WALEntry entry) throws SQLException {
        Map<String, Object> whereConditions = extractWhereConditions(entry);
        
        if (whereConditions.isEmpty()) {
            throw new SQLException("No WHERE conditions specified for DELETE");
        }
        
        List<Object> parameters = new ArrayList<>();
        
        String whereClause = whereConditions.entrySet().stream()
            .map(e -> {
                parameters.add(e.getValue());
                return e.getKey() + " = ?";
            })
            .collect(Collectors.joining(" AND "));
        
        String sql = String.format("DELETE FROM %s.%s WHERE %s",
            entry.getDbName(),
            entry.getTableName(),
            whereClause
        );
        
        logger.debug("Executing DELETE: {}", sql);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                setParameter(stmt, i + 1, parameters.get(i));
            }
            return stmt.executeUpdate();
        }
    }
    
    private int executeUpsert(Connection conn, WALEntry entry) throws SQLException {
        List<String> columns = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        List<String> updateColumns = new ArrayList<>();
        
        for (Map.Entry<String, Object> dataEntry : entry.getData().entrySet()) {
            // Skip internal fields and WHERE conditions
            if (!dataEntry.getKey().startsWith("_") && !dataEntry.getKey().startsWith("where_")) {
                columns.add(dataEntry.getKey());
                values.add(dataEntry.getValue());
                
                // Don't update primary key columns
                if (!dataEntry.getKey().startsWith("id_") && !dataEntry.getKey().equals("id")) {
                    updateColumns.add(dataEntry.getKey());
                }
            }
        }
        
        String updateClause = updateColumns.stream()
            .map(col -> col + " = VALUES(" + col + ")")
            .collect(Collectors.joining(", "));
        
        String sql = String.format("INSERT INTO %s.%s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
            entry.getDbName(),
            entry.getTableName(),
            String.join(", ", columns),
            columns.stream().map(c -> "?").collect(Collectors.joining(", ")),
            updateClause
        );
        
        logger.debug("Executing UPSERT: {}", sql);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                setParameter(stmt, i + 1, values.get(i));
            }
            return stmt.executeUpdate();
        }
    }
    
    private Map<String, Object> extractWhereConditions(WALEntry entry) {
        return entry.getData().entrySet().stream()
            .filter(e -> e.getKey().startsWith("where_"))
            .collect(Collectors.toMap(
                e -> e.getKey().substring(6), // Remove "where_" prefix
                Map.Entry::getValue
            ));
    }
    
    private Map<String, Object> extractSetValues(WALEntry entry) {
        return entry.getData().entrySet().stream()
            .filter(e -> !e.getKey().startsWith("where_") && !e.getKey().startsWith("_"))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
            ));
    }
    
    private void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, java.sql.Types.NULL);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof Double) {
            stmt.setDouble(index, (Double) value);
        } else if (value instanceof Boolean) {
            stmt.setBoolean(index, (Boolean) value);
        } else if (value instanceof java.math.BigDecimal) {
            stmt.setBigDecimal(index, (java.math.BigDecimal) value);
        } else {
            // Default to string representation
            stmt.setString(index, value.toString());
        }
    }
}