package com.telcobright.core.cache.api;

/**
 * Listener for cache events.
 * Allows external components to react to cache operations.
 */
public interface CacheEventListener {
    
    /**
     * Called before a batch operation is processed.
     * 
     * @param event The pre-operation event
     */
    void onBeforeOperation(CacheEvent event);
    
    /**
     * Called after a batch operation is completed.
     * 
     * @param event The post-operation event
     */
    void onAfterOperation(CacheEvent event);
    
    /**
     * Called when an error occurs during operation.
     * 
     * @param event The error event
     */
    void onError(CacheEvent event);
    
    /**
     * Called when cache is initialized.
     */
    void onCacheInitialized();
    
    /**
     * Called when cache is shutting down.
     */
    void onCacheShutdown();
    
    /**
     * Cache event containing operation details.
     */
    class CacheEvent {
        private final String transactionId;
        private final EventType type;
        private final Object data;
        private final long timestamp;
        private final Exception error;
        
        public enum EventType {
            BATCH_START,
            BATCH_COMPLETE,
            BATCH_ERROR,
            WAL_WRITE,
            CACHE_UPDATE,
            DB_SYNC,
            REPLAY_START,
            REPLAY_COMPLETE
        }
        
        public CacheEvent(String transactionId, EventType type, Object data) {
            this(transactionId, type, data, null);
        }
        
        public CacheEvent(String transactionId, EventType type, Object data, Exception error) {
            this.transactionId = transactionId;
            this.type = type;
            this.data = data;
            this.error = error;
            this.timestamp = System.currentTimeMillis();
        }
        
        // Getters
        public String getTransactionId() { return transactionId; }
        public EventType getType() { return type; }
        public Object getData() { return data; }
        public long getTimestamp() { return timestamp; }
        public Exception getError() { return error; }
    }
}