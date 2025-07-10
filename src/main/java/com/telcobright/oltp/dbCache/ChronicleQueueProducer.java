package com.telcobright.oltp.dbCache;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ChronicleQueueProducer {

    private ChronicleQueue queue;

    @ConfigProperty(name = "chronicle.queue.path")
    String queuePath;

    @Produces
    @ApplicationScoped
    public ExcerptAppender produceExcerptAppender() {
        queue = ChronicleQueue.singleBuilder(queuePath).build();
        return queue.acquireAppender();
    }

    @PreDestroy
    public void cleanup() {
        if (queue != null) {
            queue.close();
        }
    }
}
