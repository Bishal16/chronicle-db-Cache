package com.telcobright.oltp.queue.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChronicleInstance {
    private final ChronicleQueue queue;
    private final ExcerptAppender appender;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final List<ConsumerToQueue> consumers = new ArrayList<>();
    private static final Logger logger = LoggerFactory.getLogger(ChronicleInstance.class);

    public ChronicleInstance(String queuePath, RollCycle rollCycle) {
        this.queue = SingleChronicleQueueBuilder.single(queuePath)
                .rollCycle(rollCycle)
                .build();
        this.appender = queue.acquireAppender();
        logger.info("✅ Chronicle Queue started at path={}, rollCycle={}", queuePath, rollCycle);
    }

    public ExcerptAppender getAppender() {
        return appender;
    }

    public ChronicleQueue getQueue() {
        return queue;
    }

    public void subscribe(ConsumerToQueue consumer) {
        consumers.add(consumer);
        executorService.submit(consumer);
    }

    public void shutdown() {
        consumers.forEach(ConsumerToQueue::shutdown);
        executorService.shutdown();
        queue.close();
        logger.info("✅ Chronicle queue and all consumers shutdown complete.");
    }
}
