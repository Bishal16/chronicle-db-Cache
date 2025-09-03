# Chronicle Queue Corruption Handling for Reader/Consumer

## 🔴 **Scenario 1: Corruption During Startup Replay**

### **The Problem**
When the application starts, it needs to replay the WAL to rebuild the in-memory cache. If corruption is encountered during this replay, the reader must decide how to proceed.

### **Detection Point**
```java
public void replayWAL() {
    try (ExcerptTailer tailer = queue.createTailer()) {
        tailer.toStart(); // Start from beginning
        
        while (tailer.readDocument(wire -> {
            // Attempt to read each entry
            String transactionId = wire.read("transactionId").text();
            // ... process entry
        })) {
            // Continue reading
        }
    } catch (Exception e) {
        // CORRUPTION DETECTED!
        handleStartupCorruption(e);
    }
}
```

### **Recovery Strategy: Skip to Last Known Good Position**

```java
private void handleStartupCorruption(Exception e) {
    logger.error("❌ WAL corruption detected during startup replay", e);
    
    // Step 1: Find the last successfully processed index
    long lastGoodIndex = findLastGoodIndex();
    
    // Step 2: Get checkpoint from database
    Checkpoint checkpoint = getLastCheckpoint();
    
    // Step 3: Decide recovery strategy
    if (checkpoint != null && checkpoint.walIndex > lastGoodIndex) {
        // Use checkpoint as it's more recent
        logger.info("📌 Using checkpoint at index: {}", checkpoint.walIndex);
        recoverFromCheckpoint(checkpoint);
    } else {
        // Use last good index
        logger.info("📌 Using last good index: {}", lastGoodIndex);
        recoverFromIndex(lastGoodIndex);
    }
}

private void recoverFromCheckpoint(Checkpoint checkpoint) {
    // 1. Clear cache
    cache.clear();
    
    // 2. Reload from database up to checkpoint
    String sql = """
        SELECT * FROM packageaccount 
        WHERE last_modified <= ?
        """;
    loadFromDatabase(sql, checkpoint.timestamp);
    
    // 3. Create new tailer starting AFTER checkpoint
    try (ExcerptTailer tailer = queue.createTailer()) {
        tailer.moveToIndex(checkpoint.walIndex + 1);
        
        // 4. Continue replay from checkpoint
        while (true) {
            try {
                if (!tailer.readDocument(wire -> processEntry(wire))) {
                    break; // No more entries
                }
            } catch (Exception e) {
                // Skip corrupted entry
                logger.warn("Skipping corrupted entry at index: {}", tailer.index());
                tailer.moveToIndex(tailer.index() + 1);
            }
        }
    }
}

private long findLastGoodIndex() {
    long lastGood = -1;
    
    try (ExcerptTailer tailer = queue.createTailer()) {
        tailer.toStart();
        
        while (true) {
            long currentIndex = tailer.index();
            try {
                if (!tailer.readDocument(wire -> {
                    // Try to read basic fields
                    wire.read("transactionId").text();
                })) {
                    break;
                }
                lastGood = currentIndex; // This index was good
            } catch (Exception e) {
                // Found corruption point
                logger.info("Corruption starts at index: {}", currentIndex);
                break;
            }
        }
    }
    
    return lastGood;
}
```

## 🔴 **Scenario 2: Corruption During Runtime Consumption**

### **The Problem**
While the application is running and consuming new entries from the WAL, it encounters a corrupted entry.

### **Detection Point**
```java
public void consumeInRuntime() {
    while (isRunning) {
        try {
            boolean hasData = tailer.readDocument(wire -> {
                processEntry(wire);
            });
            
            if (!hasData) {
                Thread.sleep(10); // Wait for new data
            }
        } catch (Exception e) {
            // CORRUPTION DETECTED IN RUNTIME!
            handleRuntimeCorruption(e, tailer.index());
        }
    }
}
```

### **Recovery Strategy: Skip and Continue**

```java
private void handleRuntimeCorruption(Exception e, long corruptedIndex) {
    logger.error("❌ Runtime corruption at index: {}", corruptedIndex, e);
    
    // Step 1: Record the corruption event
    recordCorruptionEvent(corruptedIndex, e);
    
    // Step 2: Try to skip the corrupted entry
    boolean recovered = skipCorruptedEntry(tailer, corruptedIndex);
    
    if (!recovered) {
        // Step 3: If can't skip, jump to end of queue
        handleSevereCorruption(tailer);
    }
}

private boolean skipCorruptedEntry(ExcerptTailer tailer, long corruptedIndex) {
    try {
        // Method 1: Try to move to next index
        tailer.moveToIndex(corruptedIndex + 1);
        
        // Test if we can read from new position
        boolean canRead = tailer.readDocument(wire -> {
            wire.read("transactionId").text(); // Test read
        });
        
        if (canRead) {
            logger.info("✅ Successfully skipped corrupted entry");
            // Move back to continue normal processing
            tailer.moveToIndex(corruptedIndex + 1);
            return true;
        }
    } catch (Exception e) {
        logger.warn("Cannot skip to next entry: {}", e.getMessage());
    }
    
    // Method 2: Try to find next valid entry
    return findNextValidEntry(tailer, corruptedIndex);
}

private boolean findNextValidEntry(ExcerptTailer tailer, long startIndex) {
    long maxAttempts = 100; // Don't search forever
    
    for (long i = 1; i <= maxAttempts; i++) {
        try {
            tailer.moveToIndex(startIndex + i);
            
            boolean canRead = tailer.readDocument(wire -> {
                wire.read("transactionId").text();
            });
            
            if (canRead) {
                logger.info("✅ Found valid entry at offset +{}", i);
                tailer.moveToIndex(startIndex + i);
                return true;
            }
        } catch (Exception e) {
            // Continue searching
        }
    }
    
    logger.warn("❌ No valid entry found in next {} positions", maxAttempts);
    return false;
}

private void handleSevereCorruption(ExcerptTailer tailer) {
    logger.error("🔴 SEVERE: Cannot recover from corruption, jumping to end");
    
    // 1. Record severe corruption event
    recordSevereCorruptionEvent(tailer.index());
    
    // 2. Create database checkpoint NOW
    createEmergencyCheckpoint();
    
    // 3. Jump to end of queue (skip all corrupted data)
    tailer.toEnd();
    
    // 4. Alert operations team
    alertOperationsTeam("Severe WAL corruption - manual review needed");
    
    // 5. Continue processing new entries only
    logger.info("📍 Resumed at end of queue, waiting for new entries");
}
```

## 🛡️ **Comprehensive Runtime Protection**

```java
public class ResilientWALReader {
    private static final int MAX_CONSECUTIVE_ERRORS = 3;
    private int consecutiveErrors = 0;
    private long lastErrorIndex = -1;
    
    public void consumeWithProtection() {
        while (isRunning) {
            try {
                long currentIndex = tailer.index();
                
                boolean hasData = tailer.readDocument(wire -> {
                    processEntry(wire);
                });
                
                if (hasData) {
                    // Reset error counter on successful read
                    consecutiveErrors = 0;
                } else {
                    Thread.sleep(10);
                }
                
            } catch (Exception e) {
                long errorIndex = tailer.index();
                
                // Check if stuck on same error
                if (errorIndex == lastErrorIndex) {
                    consecutiveErrors++;
                } else {
                    consecutiveErrors = 1;
                    lastErrorIndex = errorIndex;
                }
                
                if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
                    // Definitely corrupted, take action
                    handleConfirmedCorruption(errorIndex);
                    consecutiveErrors = 0; // Reset after handling
                } else {
                    // Might be temporary, retry
                    logger.warn("Error at index {} (attempt {})", 
                        errorIndex, consecutiveErrors);
                    Thread.sleep(100); // Brief pause before retry
                }
            }
        }
    }
    
    private void handleConfirmedCorruption(long index) {
        logger.error("🔴 Confirmed corruption at index: {}", index);
        
        // 1. Try to skip single entry
        if (!skipSingleEntry(index)) {
            // 2. Try to find next valid section
            if (!findNextValidSection(index)) {
                // 3. Jump to end as last resort
                jumpToEndWithCheckpoint();
            }
        }
    }
}
```

## 📊 **Decision Matrix**

| Scenario | Corruption Type | Action | Data Loss |
|----------|----------------|---------|-----------|
| **Startup - Early corruption** | First 10% of WAL | Load from DB, skip corrupted section | Minimal - use checkpoint |
| **Startup - Late corruption** | Last 10% of WAL | Replay until corruption, load rest from DB | Recent transactions only |
| **Runtime - Single entry** | One bad entry | Skip entry, continue | Single transaction |
| **Runtime - Block corruption** | Multiple sequential entries | Find next valid block or jump to end | Multiple transactions |
| **Runtime - Severe** | Cannot recover | Jump to end, alert ops | All corrupted section |

## 🔧 **Best Practices**

### **1. Always Maintain Position State**
```java
class PositionTracker {
    private volatile long lastSuccessfulIndex = -1;
    private volatile String lastSuccessfulTxnId = null;
    private volatile long lastCheckpointIndex = -1;
    
    void recordSuccess(long index, String txnId) {
        this.lastSuccessfulIndex = index;
        this.lastSuccessfulTxnId = txnId;
    }
}
```

### **2. Implement Circuit Breaker**
```java
class CorruptionCircuitBreaker {
    private int failureCount = 0;
    private static final int THRESHOLD = 5;
    
    boolean shouldContinue() {
        return failureCount < THRESHOLD;
    }
    
    void recordFailure() {
        failureCount++;
        if (failureCount >= THRESHOLD) {
            // Switch to degraded mode
            switchToDegradedMode();
        }
    }
}
```

### **3. Log Everything for Post-Mortem**
```java
class CorruptionLogger {
    void logCorruption(long index, Exception e) {
        // Log to separate corruption file
        String entry = String.format(
            "%s|INDEX=%d|ERROR=%s|STACK=%s%n",
            Instant.now(), index, e.getMessage(), 
            Arrays.toString(e.getStackTrace())
        );
        Files.write(Paths.get("corruption.log"), 
            entry.getBytes(), StandardOpenOption.APPEND);
    }
}
```

## 🎯 **Summary**

**During Startup:**
- Try to replay until corruption point
- Use database checkpoint to fill gaps
- Skip corrupted sections if possible
- Worst case: Start fresh from database

**During Runtime:**
- Try to skip individual corrupted entries
- Search for next valid entry within reasonable range
- Jump to end of queue if corruption is severe
- Always maintain checkpoint for recovery

**Key Principle:** Never let corruption stop the system - always find a way to continue, even if it means some data loss. Document what was skipped for manual recovery later.