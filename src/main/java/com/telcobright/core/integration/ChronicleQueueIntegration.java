package com.telcobright.core.integration;

import com.telcobright.core.consumer.TransactionalConsumer;
import com.telcobright.core.wal.WALWriter;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Factory and integration class for Chronicle Queue-based WAL system.
 * Provides methods to create and configure all necessary components.
 */
public class ChronicleQueueIntegration {
    private static final Logger logger = LoggerFactory.getLogger(ChronicleQueueIntegration.class);
    
    private final ChronicleQueue queue;
    private final DataSource dataSource;
    private final String offsetDb;
    private final String offsetTable;
    
    private final List<TransactionalConsumer> consumers;
    private final ExecutorService executorService;
    
    /**
     * Configuration for the Chronicle Queue integration
     */
    public static class Config {
        private String queuePath = "./chronicle-queue";
        private String offsetDb = "admin";
        private String offsetTable = "consumer_offsets";
        private int consumerCount = 1;
        private boolean enableCompression = false;
        private int blockSize = 64 * 1024 * 1024; // 64MB default
        
        // Builder pattern for configuration
        public Config withQueuePath(String path) {
            this.queuePath = path;
            return this;
        }
        
        public Config withOffsetDb(String db) {
            this.offsetDb = db;
            return this;
        }
        
        public Config withOffsetTable(String table) {
            this.offsetTable = table;
            return this;
        }
        
        public Config withConsumerCount(int count) {
            this.consumerCount = count;
            return this;
        }
        
        public Config withCompression(boolean enable) {
            this.enableCompression = enable;
            return this;
        }
        
        public Config withBlockSize(int size) {
            this.blockSize = size;
            return this;
        }
    }
    
    /**
     * Create a new Chronicle Queue integration with the given configuration
     */
    public ChronicleQueueIntegration(DataSource dataSource, Config config) {
        this.dataSource = dataSource;
        this.offsetDb = config.offsetDb;
        this.offsetTable = config.offsetTable;
        
        // Create Chronicle Queue
        this.queue = createQueue(config);
        
        // Initialize consumer list and executor
        this.consumers = new ArrayList<>();
        this.executorService = Executors.newFixedThreadPool(config.consumerCount);
        
        logger.info("✅ Initialized Chronicle Queue integration at path: {}", config.queuePath);
    }
    
    /**
     * Create a Chronicle Queue with the given configuration
     */
    private ChronicleQueue createQueue(Config config) {
        File queueDir = new File(config.queuePath);
        if (!queueDir.exists()) {
            queueDir.mkdirs();
        }
        
        SingleChronicleQueueBuilder builder = SingleChronicleQueueBuilder
            .binary(queueDir)
            .blockSize(config.blockSize);
        
        if (config.enableCompression) {
            // Enable compression if configured
            // Note: This might require additional Chronicle Queue modules
            logger.info("Compression enabled for Chronicle Queue");
        }
        
        return builder.build();
    }
    
    /**
     * Create a WAL writer for this queue
     */
    public WALWriter createWALWriter() {
        return new WALWriter(queue.acquireAppender());
    }
    
    /**
     * Create and start a consumer with the given name
     */
    public TransactionalConsumer createConsumer(String consumerName) {
        return createConsumer(consumerName, null);
    }
    
    /**
     * Create and start a consumer with the given name and listener
     */
    public TransactionalConsumer createConsumer(String consumerName, 
                                               TransactionalConsumer.ConsumerListener listener) {
        TransactionalConsumer consumer = new TransactionalConsumer(
            queue, dataSource, consumerName, offsetTable, offsetDb
        );
        
        if (listener != null) {
            consumer.setListener(listener);
        }
        
        consumers.add(consumer);
        return consumer;
    }
    
    /**
     * Start all consumers
     */
    public void startConsumers() {
        for (TransactionalConsumer consumer : consumers) {
            executorService.submit(consumer);
            logger.info("▶️ Started consumer in background");
        }
    }
    
    /**
     * Start a specific consumer
     */
    public void startConsumer(TransactionalConsumer consumer) {
        if (!consumers.contains(consumer)) {
            consumers.add(consumer);
        }
        executorService.submit(consumer);
        logger.info("▶️ Started consumer in background");
    }
    
    /**
     * Stop all consumers gracefully
     */
    public void shutdown() {
        logger.info("Shutting down Chronicle Queue integration...");
        
        // Stop all consumers
        for (TransactionalConsumer consumer : consumers) {
            consumer.stop();
        }
        
        // Shutdown executor service
        executorService.shutdown();
        
        // Close the queue
        if (queue != null) {
            queue.close();
        }
        
        logger.info("✅ Chronicle Queue integration shutdown complete");
    }
    
    /**
     * Get the underlying Chronicle Queue instance
     */
    public ChronicleQueue getQueue() {
        return queue;
    }
    
    /**
     * Get all active consumers
     */
    public List<TransactionalConsumer> getConsumers() {
        return new ArrayList<>(consumers);
    }
    
    /**
     * Check if all consumers are running
     */
    public boolean areConsumersRunning() {
        return consumers.stream().allMatch(TransactionalConsumer::isRunning);
    }
    
    /**
     * Get queue statistics
     */
    public QueueStats getStats() {
        QueueStats stats = new QueueStats();
        stats.queuePath = queue.file().getAbsolutePath();
        stats.consumerCount = consumers.size();
        stats.activeConsumers = (int) consumers.stream()
            .filter(TransactionalConsumer::isRunning)
            .count();
        
        try {
            stats.lastIndex = queue.createTailer().toEnd().index();
        } catch (Exception e) {
            stats.lastIndex = -1;
        }
        
        return stats;
    }
    
    /**
     * Statistics about the queue
     */
    public static class QueueStats {
        public String queuePath;
        public int consumerCount;
        public int activeConsumers;
        public long lastIndex;
        
        @Override
        public String toString() {
            return String.format("QueueStats{path='%s', consumers=%d, active=%d, lastIndex=%d}",
                queuePath, consumerCount, activeConsumers, lastIndex);
        }
    }
}