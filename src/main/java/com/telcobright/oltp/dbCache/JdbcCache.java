package com.telcobright.oltp.dbCache;

import com.zaxxer.hikari.HikariDataSource;
import net.openhft.chronicle.queue.ChronicleQueue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public abstract class JdbcCache<Tkey, TEntity, TDelta> {
    protected ConcurrentHashMap<Tkey,TEntity> pkgIdVsPkgAccountCache = new ConcurrentHashMap<>();
    protected String jdbcUrl;
    protected HikariDataSource dataSource;
    ChronicleQueue queue;

    // No-args constructor needed for CDI proxying
    protected JdbcCache() {
    }

    protected JdbcCache(String jdbcUrl, HikariDataSource dataSource) {
        if ((jdbcUrl == null || jdbcUrl.isBlank()) && dataSource == null) {
            throw new IllegalArgumentException("Either jdbcUrl or HikariDataSource must be provided.");
        }
        this.jdbcUrl = jdbcUrl;
        this.dataSource = dataSource;
    }

    // Setter for dataSource to support lazy injection
    protected void setDataSource(HikariDataSource dataSource) {
        if (this.dataSource != null) {
            throw new IllegalStateException("DataSource already set");
        }
        this.dataSource = dataSource;
    }

    // Setter for jdbcUrl if needed
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
    protected abstract Consumer<TDelta> getUpdateAction();
    public void update(TDelta delta){
        writeWALForUpdate(delta);
        getUpdateAction().accept(delta);
    }

    protected abstract void writeWALForInsert(TEntity entity);
    protected abstract Consumer<TEntity> getInsertAction();
    public void insert(TEntity entity){
        writeWALForInsert(entity);
        getInsertAction().accept(entity);
    }
}



//package com.telcobright.oltp.dbCache;
//
//import com.zaxxer.hikari.HikariDataSource;
//import net.openhft.chronicle.queue.ChronicleQueue;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.function.Consumer;
//
//public abstract class JdbcCache<Tkey, TEntity, TDelta> {
//    protected ConcurrentHashMap<Tkey,TEntity> entities= new ConcurrentHashMap<>();
//    private final String jdbcUrl;
//    private final HikariDataSource dataSource;
//    ChronicleQueue queue;
//
//    protected JdbcCache(String jdbcUrl, HikariDataSource dataSource) {
//        if ((jdbcUrl == null || jdbcUrl.isBlank()) && dataSource == null) {
//            throw new IllegalArgumentException("Either jdbcUrl or HikariDataSource must be provided.");
//        }
//        this.jdbcUrl = jdbcUrl;
//        this.dataSource = dataSource;
//    }
//
//    protected Connection getConnection() throws SQLException {
//        if (dataSource != null) {
//            return dataSource.getConnection();
//        } else {
//            return DriverManager.getConnection(jdbcUrl);
//        }
//    }
//
//    public abstract void initFromDb() throws SQLException;
//    protected abstract void writeWALForUpdate(TDelta delta);
//    protected abstract Consumer<TDelta> getUpdateAction();
//    public void update(TDelta delta){
//        writeWALForUpdate(delta);
//        getUpdateAction().accept(delta);
//    }
//
//    protected abstract void writeWALForInsert(TEntity entity);
//    protected abstract Consumer<TEntity> getInsertAction();
//    public void insert(TEntity entity){
//        writeWALForInsert(entity);
//        getInsertAction().accept(entity);
//    }
//
//}
