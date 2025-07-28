package com.telcobright.oltp.service;

import com.telcobright.oltp.dbCache.PackageAccountCache;
import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import com.zaxxer.hikari.HikariDataSource;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Startup
@ApplicationScoped
public class ConsumerFactory {
    private final String offsetTable;
    private final boolean replayOnStart;
    private final HikariDataSource dataSource;
    private final ChronicleInstance chronicleInstance;
    private final PackageAccountCache packageAccountCache;

    @Inject
    public ConsumerFactory(
            @ConfigProperty(name = "chronicle.queue.offset.table", defaultValue = "queue_offsets") String offsetTable,
            @ConfigProperty(name = "chronicle.queue.replay.on.start", defaultValue = "true") boolean replayOnStart,
            HikariDataSource dataSource,
            ChronicleInstance chronicleInstance,
            PackageAccountCache packageAccountCache
    ) {
        this.offsetTable = offsetTable;
        this.replayOnStart = replayOnStart;
        this.dataSource = dataSource;
        this.chronicleInstance = chronicleInstance;
        this.packageAccountCache = packageAccountCache;
    }
    @Inject
    PendingStatusChecker pendingStatusChecker;

    @PostConstruct
    public void createConsumersAndSubscribe() {
        PrepaidConsumer consumer = new PrepaidConsumer(
                chronicleInstance.getQueue(),
                chronicleInstance.getAppender(),
                "prepaid-consumer",
                dataSource,
                offsetTable,
                replayOnStart,
                pendingStatusChecker,
                packageAccountCache
        );

        chronicleInstance.subscribe(consumer);
    }
}
