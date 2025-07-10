package com.telcobright.oltp.dbCache;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public abstract class JdbcCache<Tkey, TEntity, TDelta> {
    protected ConcurrentHashMap<Tkey,TEntity> pkgIdVsPkgAccountCache = new ConcurrentHashMap<>();
    protected String jdbcUrl;
    protected HikariDataSource dataSource;

    protected JdbcCache() {
    }

    protected JdbcCache(String jdbcUrl, HikariDataSource dataSource) {
        if ((jdbcUrl == null || jdbcUrl.isBlank()) && dataSource == null) {
            throw new IllegalArgumentException("Either jdbcUrl or HikariDataSource must be provided.");
        }
        this.jdbcUrl = jdbcUrl;
        this.dataSource = dataSource;
    }

    protected void setDataSource(HikariDataSource dataSource) {
        if (this.dataSource != null) {
            throw new IllegalStateException("DataSource already set");
        }
        this.dataSource = dataSource;
    }

    protected void setJdbcUrl(String jdbcUrl) {
        if (this.jdbcUrl != null) {
            throw new IllegalStateException("jdbcUrl already set");
        }
        this.jdbcUrl = jdbcUrl;
    }

    protected Connection getConnection() throws SQLException {
        if (dataSource != null) {
            return dataSource.getConnection();
        } else {
            return DriverManager.getConnection(jdbcUrl);
        }
    }

    public abstract void initFromDb() throws SQLException;

    protected abstract void writeWALForUpdate(TDelta delta);
    protected abstract Consumer<TDelta> updateCache();
    public void update(TDelta delta){
        writeWALForUpdate(delta);
        updateCache().accept(delta);
    }

    protected abstract void writeWALForInsert(TEntity entity);
    protected abstract Consumer<TEntity> getInsertAction();
    public void insert(TEntity entity){
        writeWALForInsert(entity);
        getInsertAction().accept(entity);
    }
}

