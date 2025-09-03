package com.telcobright.core.cache.client;

import com.telcobright.core.cache.client.api.CacheClient;
import com.telcobright.core.cache.EntityCache;

import java.util.Map;

/**
 * Configuration for cache client creation.
 */
public class CacheClientConfig {
    
    private CacheClient.ClientType clientType;
    
    // For embedded client
    @SuppressWarnings("rawtypes")
    private Map<String, EntityCache> cacheRegistry;
    
    // For gRPC client
    private String host;
    private int port;
    
    // For REST client
    private String baseUrl;
    
    // Common configuration
    private long timeoutMs = 30000; // Default 30 seconds
    private int maxRetries = 3;
    private boolean compression = false;
    private boolean enableMetrics = false;
    
    // Authentication
    private String authToken;
    private String username;
    private String password;
    
    // Connection pooling
    private int maxConnections = 10;
    private int connectionTimeoutMs = 5000;
    
    // Getters and setters
    
    public CacheClient.ClientType getClientType() {
        return clientType;
    }
    
    public void setClientType(CacheClient.ClientType clientType) {
        this.clientType = clientType;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K, V> Map<String, EntityCache<K, V>> getCacheRegistry() {
        return (Map) cacheRegistry;
    }
    
    @SuppressWarnings("rawtypes")
    public void setCacheRegistry(Map<String, EntityCache> cacheRegistry) {
        this.cacheRegistry = cacheRegistry;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public String getBaseUrl() {
        return baseUrl;
    }
    
    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
    
    public long getTimeoutMs() {
        return timeoutMs;
    }
    
    public void setTimeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
    public boolean isCompression() {
        return compression;
    }
    
    public void setCompression(boolean compression) {
        this.compression = compression;
    }
    
    public boolean isEnableMetrics() {
        return enableMetrics;
    }
    
    public void setEnableMetrics(boolean enableMetrics) {
        this.enableMetrics = enableMetrics;
    }
    
    public String getAuthToken() {
        return authToken;
    }
    
    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public int getMaxConnections() {
        return maxConnections;
    }
    
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }
    
    public int getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }
    
    public void setConnectionTimeoutMs(int connectionTimeoutMs) {
        this.connectionTimeoutMs = connectionTimeoutMs;
    }
}