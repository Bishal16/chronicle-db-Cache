package com.telcobright.oltp.service;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class HikariCPConnectionPool {

    private static HikariDataSource dataSource;

    public static HikariDataSource initialize(String jdbcUrl, String username, String password, int poolSize) {
        if (dataSource != null) {
            return dataSource;  // Prevent re-initialization
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(poolSize);

        dataSource = new HikariDataSource(config);
        return dataSource;
    }

    public static HikariDataSource getDataSource() {
        return dataSource;
    }
}
