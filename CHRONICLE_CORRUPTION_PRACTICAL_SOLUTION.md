# Chronicle Queue Corruption - Practical Solution for High-Volume Systems

## ❌ **Why Database Reload is NOT Feasible**

When you have millions of pending operations in the queue:
- **Performance Impact**: Loading millions of records from DB takes hours
- **Memory Pressure**: Can't fit everything in memory at once
- **Defeats Purpose**: Chronicle Queue exists to AVOID database bottlenecks
- **Business Impact**: System downtime while loading

## ✅ **The Practical Solution: Skip and Accept Loss**

### **Core Strategy: Forward-Only Recovery**

```
Normal Flow:
[Entry1] → [Entry2] → [Entry3] → [Corrupted] → [Entry5] → [Entry6] → ...
                                       ↓
                                    SKIP IT
                                       ↓
                            Continue from Entry5
```

### **Implementation: Aggressive Skip Strategy**

```java
public class PracticalCorruptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(PracticalCorruptionHandler.class);
    
    public void handleStartupReplay() {
        long totalEntries = 0;
        long skippedEntries = 0;
        long lastSuccessIndex = -1;
        
        try (ExcerptTailer tailer = queue.createTailer()) {
            tailer.toStart();
            
            while (!tailer.atEnd()) {
                long currentIndex = tailer.index();
                
                try {
                    boolean hasData = tailer.readDocument(wire -> {
                        processEntry(wire);
                    });
                    
                    if (hasData) {
                        totalEntries++;
                        lastSuccessIndex = currentIndex;
                    }
                    
                } catch (Exception e) {
                    // DON'T STOP - SKIP AND CONTINUE
                    skippedEntries++;
                    logger.warn("Skipping corrupted entry at index: {} ({})", 
                        currentIndex, e.getMessage());
                    
                    // Try to jump forward
                    if (!jumpOverCorruption(tailer, currentIndex)) {
                        // Can't recover, exit replay
                        logger.error("Cannot skip corruption at index: {}", currentIndex);
                        break;
                    }
                }
            }
        }
        
        logger.info("Replay complete: processed={}, skipped={}, loss={}%", 
            totalEntries, skippedEntries, 
            (skippedEntries * 100.0) / (totalEntries + skippedEntries));
            
        if (skippedEntries > 0) {
            recordDataLoss(skippedEntries, totalEntries);
        }
    }
    
    private boolean jumpOverCorruption(ExcerptTailer tailer, long badIndex) {
        // Strategy: Try increasing jumps
        int[] jumpSizes = {1, 10, 100, 1000, 10000};
        
        for (int jumpSize : jumpSizes) {
            try {
                long targetIndex = badIndex + jumpSize;
                tailer.moveToIndex(targetIndex);
                
                // Test if we can read
                AtomicBoolean canRead = new AtomicBoolean(false);
                tailer.readDocument(wire -> {
                    wire.read("transactionId").text(); // Test read
                    canRead.set(true);
                });
                
                if (canRead.get()) {
                    logger.info("✅ Jumped over corruption: {} entries skipped", jumpSize);
                    return true;
                }
            } catch (Exception e) {
                // Try next jump size
            }
        }
        
        // Last resort: Jump to a much later position
        return jumpToSafePosition(tailer, badIndex);
    }
    
    private boolean jumpToSafePosition(ExcerptTailer tailer, long badIndex) {
        try {
            // Jump 1 million entries forward
            long safeIndex = badIndex + 1_000_000;
            tailer.moveToIndex(safeIndex);
            
            if (!tailer.atEnd()) {
                logger.warn("⚠️ MAJOR SKIP: Jumped 1M entries forward from corruption");
                return true;
            }
            
            // If that's beyond end, go to actual end
            tailer.toEnd();
            logger.warn("⚠️ Jumped to end of queue from corruption point");
            return true;
            
        } catch (Exception e) {
            return false;
        }
    }
}
```

## 📊 **Data Loss Tracking & Monitoring**

### **1. Loss Tracking Table**
```sql
CREATE TABLE chronicle_data_loss (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    occurrence_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    loss_type ENUM('CORRUPTION_SKIP', 'MANUAL_SKIP', 'EMERGENCY_JUMP'),
    start_index BIGINT,
    end_index BIGINT,
    estimated_entries_lost INT,
    recovery_action VARCHAR(255),
    INDEX idx_time (occurrence_time)
);
```

### **2. Loss Recording**
```java
private void recordDataLoss(long skipped, long processed) {
    String sql = """
        INSERT INTO chronicle_data_loss 
        (loss_type, estimated_entries_lost, recovery_action)
        VALUES ('CORRUPTION_SKIP', ?, 'Skipped corrupted entries during replay')
        """;
    
    try (Connection conn = dataSource.getConnection();
         PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setLong(1, skipped);
        stmt.executeUpdate();
        
        // Alert if loss is significant
        double lossPercent = (skipped * 100.0) / (processed + skipped);
        if (lossPercent > 1.0) {  // More than 1% loss
            alertHighDataLoss(lossPercent, skipped);
        }
    } catch (SQLException e) {
        logger.error("Failed to record data loss", e);
    }
}
```

## 🚀 **Runtime Corruption - Minimal Impact Strategy**

```java
public class RuntimeCorruptionHandler {
    private final AtomicLong totalSkipped = new AtomicLong(0);
    private final AtomicLong lastSkipTime = new AtomicLong(0);
    
    public void handleRuntimeConsumption() {
        ExcerptTailer tailer = queue.createTailer();
        tailer.toEnd(); // Start from end for real-time processing
        
        while (isRunning) {
            try {
                boolean hasData = tailer.readDocument(wire -> {
                    processEntry(wire);
                });
                
                if (!hasData) {
                    Thread.sleep(10); // Wait for new data
                }
                
            } catch (Exception e) {
                // Runtime corruption - skip immediately
                handleRuntimeSkip(tailer, e);
            }
        }
    }
    
    private void handleRuntimeSkip(ExcerptTailer tailer, Exception e) {
        long currentIndex = tailer.index();
        totalSkipped.incrementAndGet();
        lastSkipTime.set(System.currentTimeMillis());
        
        logger.error("Runtime corruption at index: {}, total skipped: {}", 
            currentIndex, totalSkipped.get());
        
        // Simple strategy: Move to next index
        try {
            tailer.moveToIndex(currentIndex + 1);
        } catch (Exception moveError) {
            // If can't move to next, jump to end
            tailer.toEnd();
            logger.error("Jumped to end due to corruption at: {}", currentIndex);
        }
        
        // Log for manual recovery if needed
        logSkippedEntry(currentIndex, e);
    }
    
    private void logSkippedEntry(long index, Exception e) {
        // Write to a separate file for potential manual recovery
        String logEntry = String.format("%s|INDEX=%d|ERROR=%s%n",
            Instant.now(), index, e.getMessage());
        
        try {
            Files.write(Paths.get("skipped-entries.log"),
                logEntry.getBytes(),
                StandardOpenOption.CREATE, 
                StandardOpenOption.APPEND);
        } catch (IOException ioe) {
            logger.error("Failed to log skipped entry", ioe);
        }
    }
}
```

## 🎯 **The Practical Recovery Philosophy**

### **Accept and Continue**
1. **Accept Loss**: Some data loss is better than system downtime
2. **Continue Forward**: Always keep processing new data
3. **Track Everything**: Log what was lost for analysis
4. **Alert on Threshold**: Notify when loss exceeds acceptable limits

### **What NOT to Do**
❌ **Don't reload millions from database** - Takes too long  
❌ **Don't stop processing** - Business continues  
❌ **Don't retry corrupted entries** - They won't fix themselves  
❌ **Don't hold everything in memory** - Will cause OOM  

### **What TO Do**
✅ **Skip corrupted entries** - Fast and simple  
✅ **Jump over bad sections** - Progressive jump strategy  
✅ **Continue from good data** - Keep system running  
✅ **Monitor and alert** - Know your loss rate  

## 📈 **Monitoring Dashboard Queries**

```sql
-- Daily data loss summary
SELECT 
    DATE(occurrence_time) as day,
    COUNT(*) as corruption_events,
    SUM(estimated_entries_lost) as total_lost,
    MAX(estimated_entries_lost) as max_single_loss
FROM chronicle_data_loss
WHERE occurrence_time > NOW() - INTERVAL 7 DAY
GROUP BY DATE(occurrence_time);

-- Alert query (run every minute)
SELECT COUNT(*) as recent_corruptions
FROM chronicle_data_loss
WHERE occurrence_time > NOW() - INTERVAL 1 HOUR
HAVING recent_corruptions > 10; -- Alert if >10 corruptions in an hour
```

## 🔧 **Configuration for Acceptable Loss**

```java
@ConfigProperty(name = "chronicle.corruption.max-acceptable-loss-percent", defaultValue = "0.1")
double maxAcceptableLossPercent; // 0.1% default

@ConfigProperty(name = "chronicle.corruption.skip-strategy", defaultValue = "AGGRESSIVE")
SkipStrategy skipStrategy; // AGGRESSIVE, CONSERVATIVE, NONE

public enum SkipStrategy {
    AGGRESSIVE,   // Skip large blocks if needed
    CONSERVATIVE, // Skip individual entries only
    NONE         // Stop on corruption (not recommended)
}
```

## 🎯 **Summary**

**The Reality**: With millions of pending operations, you CANNOT reload from database.

**The Solution**: **Skip and Continue**
- Skip corrupted entries/sections
- Accept the data loss
- Log what was skipped
- Continue processing
- Monitor loss rate
- Alert if excessive

**The Trade-off**: 
- **Lose**: Some transactions (typically <0.1%)
- **Gain**: System stays operational, no downtime, no performance impact

**Remember**: Chronicle Queue is about speed. Don't sacrifice that speed trying to recover every single corrupted entry. The business value of keeping the system running far exceeds the cost of losing a few transactions.