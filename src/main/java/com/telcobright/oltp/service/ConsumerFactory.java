package com.telcobright.oltp.service;

import com.telcobright.oltp.dbCache.CacheManager;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.zaxxer.hikari.HikariDataSource;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

// @Startup // Disabled for WALBatch testing
@ApplicationScoped
public class ConsumerFactory {
    private final String offsetTable;
    private final boolean replayOnStart;
    private final HikariDataSource dataSource;
    private final ChronicleInstance chronicleInstance;
    private final CacheManager cacheManager;

    @Inject
    public ConsumerFactory(
            @ConfigProperty(name = "chronicle.queue.offset.table", defaultValue = "queue_offsets") String offsetTable,
            @ConfigProperty(name = "chronicle.queue.replay.on.start", defaultValue = "true") boolean replayOnStart,
            HikariDataSource dataSource,
            ChronicleInstance chronicleInstance,
            CacheManager cacheManager
    ) {
        this.offsetTable = offsetTable;
        this.replayOnStart = replayOnStart;
        this.dataSource = dataSource;
        this.chronicleInstance = chronicleInstance;
        this.cacheManager = cacheManager;
    }
    @Inject
    PendingStatusChecker pendingStatusChecker;

    @PostConstruct
    public void createConsumersAndSubscribe() {
        // Create UniversalConsumer that handles all entity types based on table name in WAL
        UniversalConsumer universalConsumer = new UniversalConsumer(
                chronicleInstance.getQueue(),
                chronicleInstance.getAppender(),
                "universal-consumer",
                dataSource,
                offsetTable,
                replayOnStart,
                pendingStatusChecker,
                cacheManager
        );
        chronicleInstance.subscribe(universalConsumer);
    }
}
