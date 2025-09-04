package com.telcobright.examples.oltp.config;

import com.telcobright.core.cache.CacheEntityTypeSet;
import com.telcobright.core.cache.GenericStorageCacheManager;
import com.telcobright.core.cache.wal.api.WALProducer;
import com.telcobright.core.cache.wal.api.WALConsumer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for setting up the GenericStorage-based Cache.
 * This shows how to configure the cache system with the new unified storage architecture.
 */
@ApplicationScoped
public class CacheConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(CacheConfiguration.class);
    
    @Inject
    private WALProducer walProducer;
    
    @Inject
    private WALConsumer walConsumer;
    
    @Inject
    private DataSource dataSource;
    
    /**
     * Produces a configured GenericStorageCacheManager with unified storage.
     * This bean will be injected into GenericStorageCacheCoreImpl.
     */
    @Produces
    @ApplicationScoped
    public GenericStorageCacheManager configureCacheManager() {
        logger.info("Configuring GenericStorageCacheManager with unified storage...");
        
        // Configure entity type capacities
        Map<CacheEntityTypeSet, Integer> capacities = new HashMap<>();
        capacities.put(CacheEntityTypeSet.PACKAGE_ACCOUNT, 10000);
        capacities.put(CacheEntityTypeSet.PACKAGE_ACCOUNT_RESERVE, 10000);
        capacities.put(CacheEntityTypeSet.CUSTOMER, 5000);
        capacities.put(CacheEntityTypeSet.ORDER, 15000);
        capacities.put(CacheEntityTypeSet.PRODUCT, 2000);
        capacities.put(CacheEntityTypeSet.TRANSACTION_LOG, 20000);
        
        // Create GenericStorageCacheManager with unified storage
        GenericStorageCacheManager cacheManager = new GenericStorageCacheManager(
            walProducer,
            walConsumer,
            dataSource,
            100000,  // Total max records across all entity types
            capacities
        );
        
        logger.info("GenericStorageCacheManager configured successfully");
        logger.info("All entities will be stored in a single unified storage");
        logger.info("This ensures true atomicity for multi-entity transactions");
        
        return cacheManager;
    }
}