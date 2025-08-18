package com.telcobright.oltp.config.hikari;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class HikariInitializer {

    @ConfigProperty(name = "quarkus.datasource.jdbc.url")
    String datasourceUrl;

    @ConfigProperty(name = "quarkus.datasource.username")
    String username;

    @ConfigProperty(name = "quarkus.datasource.password")
    String password;

    private HikariDataSource hikariDataSource;

    private static final Logger logger = LoggerFactory.getLogger(HikariInitializer.class);

    @PostConstruct
    void init() {
        hikariDataSource = HikariCPConnectionPool.initialize(
                datasourceUrl, username, password, 2
        );
        logger.info("✅ HikariCP pool initialized with URL: {}", datasourceUrl);
    }

    @Produces  // ⬅️ THIS makes it injectable
    @ApplicationScoped
    HikariDataSource produceDataSource() {
        return hikariDataSource;
    }

    @PreDestroy
    void close() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
            System.out.println("✅ HikariCP pool closed");
        }
    }
}
