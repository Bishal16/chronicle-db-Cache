package com.telcobright.examples.oltp.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility to compare client-sent and server-received WAL batch logs
 * Verifies exact payload matching between client and server
 */
public class PayloadLogComparator {
    private static final Logger logger = LoggerFactory.getLogger(PayloadLogComparator.class);
    private static final String CLIENT_LOG_FILE = "client-sent-wal-batches.log";
    private static final String SERVER_LOG_FILE = "server-received-wal-batches.log";
    
    public static void main(String[] args) {
        logger.info("╔════════════════════════════════════════════════════════════════╗");
        logger.info("║                   WAL Batch Log Comparator                    ║");
        logger.info("║            Verifying Client-Server Payload Matching           ║");
        logger.info("╚════════════════════════════════════════════════════════════════╝");
        
        try {
            boolean allMatched = compareLogFiles();
            
            if (allMatched) {
                logger.info("🎉 SUCCESS: All payloads match exactly between client and server!");
            } else {
                logger.error("❌ FAILURE: Some payloads do not match between client and server!");
                System.exit(1);
            }
            
        } catch (Exception e) {
            logger.error("❌ Error during log comparison", e);
            System.exit(1);
        }
    }
    
    /**
     * Compare client and server log files for exact payload matching
     */
    private static boolean compareLogFiles() throws IOException {
        Path clientLogPath = Paths.get(CLIENT_LOG_FILE);
        Path serverLogPath = Paths.get(SERVER_LOG_FILE);
        
        // Check if files exist
        if (!Files.exists(clientLogPath)) {
            logger.error("❌ Client log file not found: {}", CLIENT_LOG_FILE);
            return false;
        }
        
        if (!Files.exists(serverLogPath)) {
            logger.error("❌ Server log file not found: {}", SERVER_LOG_FILE);
            return false;
        }
        
        // Read and parse log files
        List<WALBatchLogEntry> clientEntries = parseLogFile(CLIENT_LOG_FILE, "SENT_WAL_BATCH");
        List<WALBatchLogEntry> serverEntries = parseLogFile(SERVER_LOG_FILE, "RECEIVED_WAL_BATCH");
        
        logger.info("📊 Comparison Statistics:");
        logger.info("   Client entries: {}", clientEntries.size());
        logger.info("   Server entries: {}", serverEntries.size());
        
        if (clientEntries.size() != serverEntries.size()) {
            logger.error("❌ Entry count mismatch: Client={}, Server={}", 
                clientEntries.size(), serverEntries.size());
            return false;
        }
        
        // Compare each entry pair
        boolean allMatched = true;
        for (int i = 0; i < clientEntries.size(); i++) {
            WALBatchLogEntry clientEntry = clientEntries.get(i);
            WALBatchLogEntry serverEntry = serverEntries.get(i);
            
            boolean matched = compareEntries(clientEntry, serverEntry, i + 1);
            allMatched &= matched;
        }
        
        return allMatched;
    }
    
    /**
     * Parse log file and extract WAL batch entries
     */
    private static List<WALBatchLogEntry> parseLogFile(String filename, String entryPrefix) throws IOException {
        List<WALBatchLogEntry> entries = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(entryPrefix)) {
                    WALBatchLogEntry entry = parseLogLine(line);
                    if (entry != null) {
                        entries.add(entry);
                    }
                }
            }
        }
        
        logger.info("📖 Parsed {} entries from {}", entries.size(), filename);
        return entries;
    }
    
    /**
     * Parse individual log line into structured entry
     */
    private static WALBatchLogEntry parseLogLine(String line) {
        try {
            // Expected format: SENT_WAL_BATCH|timestamp|txnId|size|databases=db1,db2|ENTRY1:op|db.table|data|...
            String[] parts = line.split("\\|");
            
            if (parts.length < 5) {
                logger.warn("⚠️ Invalid log line format: {}", line);
                return null;
            }
            
            WALBatchLogEntry entry = new WALBatchLogEntry();
            entry.prefix = parts[0];
            entry.timestamp = parts[1];
            entry.transactionId = parts[2];
            entry.entryCount = Integer.parseInt(parts[3]);
            
            // Parse databases
            String databasesPart = parts[4];
            if (databasesPart.startsWith("databases=")) {
                String dbList = databasesPart.substring("databases=".length());
                entry.databases = Arrays.asList(dbList.split(","));
            }
            
            // Parse entries (everything after databases field)
            StringBuilder entriesData = new StringBuilder();
            for (int i = 5; i < parts.length; i++) {
                if (i > 5) entriesData.append("|");
                entriesData.append(parts[i]);
            }
            entry.entriesData = entriesData.toString();
            
            return entry;
            
        } catch (Exception e) {
            logger.error("Error parsing log line: {}", line, e);
            return null;
        }
    }
    
    /**
     * Compare two WAL batch log entries for exact matching
     */
    private static boolean compareEntries(WALBatchLogEntry client, WALBatchLogEntry server, int entryNumber) {
        logger.info("🔍 Comparing Entry #{}", entryNumber);
        logger.info("   Transaction ID: {} vs {}", client.transactionId, server.transactionId);
        
        boolean matched = true;
        
        // Compare transaction ID
        if (!client.transactionId.equals(server.transactionId)) {
            logger.error("❌ Entry #{}: Transaction ID mismatch", entryNumber);
            logger.error("   Client: {}", client.transactionId);
            logger.error("   Server: {}", server.transactionId);
            matched = false;
        } else {
            logger.info("   ✅ Transaction ID matches: {}", client.transactionId);
        }
        
        // Compare entry count
        if (client.entryCount != server.entryCount) {
            logger.error("❌ Entry #{}: Entry count mismatch", entryNumber);
            logger.error("   Client: {}", client.entryCount);
            logger.error("   Server: {}", server.entryCount);
            matched = false;
        } else {
            logger.info("   ✅ Entry count matches: {}", client.entryCount);
        }
        
        // Compare databases
        if (!client.databases.equals(server.databases)) {
            logger.error("❌ Entry #{}: Databases mismatch", entryNumber);
            logger.error("   Client: {}", client.databases);
            logger.error("   Server: {}", server.databases);
            matched = false;
        } else {
            logger.info("   ✅ Databases match: {}", client.databases);
        }
        
        // Compare entries data (most important - the actual payload)
        if (!normalizeEntriesData(client.entriesData).equals(normalizeEntriesData(server.entriesData))) {
            logger.error("❌ Entry #{}: Entries data mismatch", entryNumber);
            logger.error("   Client entries: {}", client.entriesData);
            logger.error("   Server entries: {}", server.entriesData);
            
            // Show detailed comparison
            showDetailedDataComparison(client.entriesData, server.entriesData);
            matched = false;
        } else {
            logger.info("   ✅ Entries data matches exactly!");
        }
        
        if (matched) {
            logger.info("   🎯 Entry #{} PERFECT MATCH! ✨", entryNumber);
        } else {
            logger.error("   💥 Entry #{} MISMATCH DETECTED!", entryNumber);
        }
        
        logger.info("");
        return matched;
    }
    
    /**
     * Normalize entries data for comparison (handle minor formatting differences)
     */
    private static String normalizeEntriesData(String data) {
        // Remove extra spaces and normalize formatting
        return data.trim().replaceAll("\\s+", " ");
    }
    
    /**
     * Show detailed comparison of entries data
     */
    private static void showDetailedDataComparison(String clientData, String serverData) {
        logger.info("🔬 Detailed Data Comparison:");
        
        String[] clientParts = clientData.split("\\|");
        String[] serverParts = serverData.split("\\|");
        
        int maxLen = Math.max(clientParts.length, serverParts.length);
        
        for (int i = 0; i < maxLen; i++) {
            String clientPart = i < clientParts.length ? clientParts[i] : "<MISSING>";
            String serverPart = i < serverParts.length ? serverParts[i] : "<MISSING>";
            
            if (clientPart.equals(serverPart)) {
                logger.info("   [{}] ✅ MATCH: {}", i, clientPart);
            } else {
                logger.error("   [{}] ❌ DIFF:", i);
                logger.error("       Client: {}", clientPart);
                logger.error("       Server: {}", serverPart);
            }
        }
    }
    
    /**
     * Data structure to hold parsed log entry
     */
    private static class WALBatchLogEntry {
        String prefix;
        String timestamp;
        String transactionId;
        int entryCount;
        List<String> databases;
        String entriesData;
        
        @Override
        public String toString() {
            return String.format("WALBatchLogEntry{txnId='%s', count=%d, databases=%s}", 
                transactionId, entryCount, databases);
        }
    }
    
    /**
     * Utility method to clean old log files before testing
     */
    public static void cleanLogFiles() {
        try {
            Files.deleteIfExists(Paths.get(CLIENT_LOG_FILE));
            Files.deleteIfExists(Paths.get(SERVER_LOG_FILE));
            logger.info("🧹 Cleaned old log files");
        } catch (IOException e) {
            logger.warn("Warning: Could not clean old log files", e);
        }
    }
}