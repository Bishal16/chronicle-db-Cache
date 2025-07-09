package com.telcobright.oltp.queue.chronicle;

import com.telcobright.oltp.prepaid.PrepaidConsumer;
import com.zaxxer.hikari.HikariDataSource;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.List;

@ApplicationScoped
public class ConsumerFactory {

    private final String offsetTable;
    private final boolean replayOnStart;
    private final HikariDataSource dataSource;

    public ConsumerFactory(
            @ConfigProperty(name = "chronicle.queue.offset.table", defaultValue = "queue_offsets") String offsetTable,
            @ConfigProperty(name = "chronicle.queue.replay.on.start", defaultValue = "true") boolean replayOnStart,
            HikariDataSource dataSource
    ) {
        this.offsetTable = offsetTable;
        this.replayOnStart = replayOnStart;
        this.dataSource = dataSource;
    }

    public List<ConsumerToQueue> createConsumers(ChronicleInstance instance) {
        // Pass both queue and appender to PrepaidConsumer
        return List.of(new PrepaidConsumer(
                instance.getQueue(),
                instance.getAppender(),   // <-- pass appender here
                "prepaid-consumer",
                dataSource,
                offsetTable,
                replayOnStart
        ));
    }
}
