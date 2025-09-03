package com.telcobright.core.cache.client;

import com.telcobright.core.cache.client.api.CacheClient;
import com.telcobright.core.cache.client.api.TransactionalCacheClient;
import com.telcobright.core.cache.client.impl.EmbeddedCacheClient;
import com.telcobright.core.cache.EntityCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for creating cache client instances based on configuration.
 */
public class CacheClientFactory {
    private static final Logger logger = LoggerFactory.getLogger(CacheClientFactory.class);
    
    /**
     * Create a cache client based on configuration.
     */
    public static <K, V> CacheClient<K, V> createClient(CacheClientConfig config) {
        switch (config.getClientType()) {
            case EMBEDDED:
                return createEmbeddedClient(config);
            case GRPC:
                return createGrpcClient(config);
            case REST:
                return createRestClient(config);
            default:
                throw new IllegalArgumentException("Unsupported client type: " + config.getClientType());
        }
    }
    
    /**
     * Create a transactional cache client.
     */
    public static <K, V> TransactionalCacheClient<K, V> createTransactionalClient(CacheClientConfig config) {
        switch (config.getClientType()) {
            case EMBEDDED:
                return createEmbeddedClient(config);
            case GRPC:
                return createGrpcTransactionalClient(config);
            case REST:
                return createRestTransactionalClient(config);
            default:
                throw new IllegalArgumentException("Unsupported client type: " + config.getClientType());
        }
    }
    
    /**
     * Create embedded client with direct cache access.
     */
    @SuppressWarnings("unchecked")
    private static <K, V> EmbeddedCacheClient<K, V> createEmbeddedClient(CacheClientConfig config) {
        Map<String, EntityCache<K, V>> cacheRegistry = config.getCacheRegistry();
        
        if (cacheRegistry == null || cacheRegistry.isEmpty()) {
            throw new IllegalArgumentException("Cache registry is required for embedded client");
        }
        
        logger.info("Creating embedded cache client with {} caches", cacheRegistry.size());
        return new EmbeddedCacheClient<>(cacheRegistry);
    }
    
    /**
     * Create gRPC client (placeholder for implementation).
     */
    private static <K, V> CacheClient<K, V> createGrpcClient(CacheClientConfig config) {
        // TODO: Implement GrpcCacheClient
        throw new UnsupportedOperationException("gRPC client not yet implemented");
        
        /*
        String host = config.getHost();
        int port = config.getPort();
        
        if (host == null || port <= 0) {
            throw new IllegalArgumentException("Host and port required for gRPC client");
        }
        
        logger.info("Creating gRPC cache client connecting to {}:{}", host, port);
        return new GrpcCacheClient<>(host, port, config);
        */
    }
    
    /**
     * Create gRPC transactional client (placeholder).
     */
    private static <K, V> TransactionalCacheClient<K, V> createGrpcTransactionalClient(CacheClientConfig config) {
        // TODO: Implement GrpcTransactionalCacheClient
        throw new UnsupportedOperationException("gRPC transactional client not yet implemented");
    }
    
    /**
     * Create REST client (placeholder for implementation).
     */
    private static <K, V> CacheClient<K, V> createRestClient(CacheClientConfig config) {
        // TODO: Implement RestCacheClient
        throw new UnsupportedOperationException("REST client not yet implemented");
        
        /*
        String baseUrl = config.getBaseUrl();
        
        if (baseUrl == null) {
            throw new IllegalArgumentException("Base URL required for REST client");
        }
        
        logger.info("Creating REST cache client with base URL: {}", baseUrl);
        return new RestCacheClient<>(baseUrl, config);
        */
    }
    
    /**
     * Create REST transactional client (placeholder).
     */
    private static <K, V> TransactionalCacheClient<K, V> createRestTransactionalClient(CacheClientConfig config) {
        // TODO: Implement RestTransactionalCacheClient
        throw new UnsupportedOperationException("REST transactional client not yet implemented");
    }
    
    /**
     * Builder for creating clients with fluent API.
     */
    public static class Builder<K, V> {
        private final CacheClientConfig config = new CacheClientConfig();
        
        public Builder<K, V> embedded() {
            config.setClientType(CacheClient.ClientType.EMBEDDED);
            return this;
        }
        
        public Builder<K, V> grpc(String host, int port) {
            config.setClientType(CacheClient.ClientType.GRPC);
            config.setHost(host);
            config.setPort(port);
            return this;
        }
        
        public Builder<K, V> rest(String baseUrl) {
            config.setClientType(CacheClient.ClientType.REST);
            config.setBaseUrl(baseUrl);
            return this;
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Builder<K, V> withCacheRegistry(Map<String, EntityCache<K, V>> registry) {
            config.setCacheRegistry((Map) registry);
            return this;
        }
        
        public Builder<K, V> withTimeout(long timeoutMs) {
            config.setTimeoutMs(timeoutMs);
            return this;
        }
        
        public Builder<K, V> withMaxRetries(int maxRetries) {
            config.setMaxRetries(maxRetries);
            return this;
        }
        
        public Builder<K, V> withCompression(boolean compression) {
            config.setCompression(compression);
            return this;
        }
        
        public CacheClient<K, V> build() {
            return createClient(config);
        }
        
        public TransactionalCacheClient<K, V> buildTransactional() {
            return createTransactionalClient(config);
        }
    }
}