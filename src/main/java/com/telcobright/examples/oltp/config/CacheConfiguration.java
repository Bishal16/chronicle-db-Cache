package com.telcobright.examples.oltp.config;

import com.telcobright.core.cache.impl.UniversalCacheCoreImpl;
import com.telcobright.core.cache.universal.EntityCacheHandler;
import com.telcobright.examples.oltp.handlers.PackageAccountHandler;
import com.telcobright.examples.oltp.handlers.PackageAccountReserveHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration class for setting up the Universal Cache with entity handlers.
 * This shows how to configure the cache system for a real application.
 */
@ApplicationScoped
public class CacheConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(CacheConfiguration.class);
    
    /**
     * Produces a configured UniversalCacheCoreImpl with all entity handlers registered.
     * This bean can be injected as CacheCoreAPI throughout the application.
     */
    @Produces
    @ApplicationScoped
    public UniversalCacheCoreImpl configureCacheCore() {
        logger.info("Configuring Universal Cache Core with entity handlers...");
        
        // Create the cache core implementation
        UniversalCacheCoreImpl cacheCore = new UniversalCacheCoreImpl() {
            @Override
            protected void registerDefaultHandlers() {
                logger.info("Registering entity handlers...");
                
                // Register handler for PackageAccount entities
                registerHandler(new PackageAccountHandler());
                logger.info("Registered PackageAccountHandler");
                
                // Register handler for PackageAccountReserve entities
                registerHandler(new PackageAccountReserveHandler());
                logger.info("Registered PackageAccountReserveHandler");
                
                // Add more handlers as needed for other entity types
                // registerHandler(new CustomEntityHandler());
            }
        };
        
        logger.info("Universal Cache Core configured successfully");
        return cacheCore;
    }
    
    /**
     * Example of how to create a custom handler for a new entity type
     */
    public static class CustomEntityHandler extends com.telcobright.core.cache.universal.GenericEntityHandler<Long, Object> {
        public CustomEntityHandler() {
            super("customTable", Object.class, "id");
        }
        
        @Override
        protected Long extractKey(com.telcobright.core.wal.WALEntry entry) {
            Object id = entry.get("id");
            return id instanceof Long ? (Long) id : Long.valueOf(id.toString());
        }
        
        @Override
        protected Object convertToEntity(com.telcobright.core.wal.WALEntry entry) {
            // Convert WAL entry to your custom entity type
            return entry.getData(); // Simplified - return the data map
        }
    }
}