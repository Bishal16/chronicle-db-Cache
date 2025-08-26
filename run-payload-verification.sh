#!/bin/bash

# WALEntryBatch Payload Verification Test Script
# Tests client->gRPC->server flow with exact payload matching verification

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║              WALEntryBatch Payload Verification                ║"
echo "║        Complete Client->gRPC->Server Testing Flow             ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${2}${1}${NC}"
}

# Function to check if process is running
is_running() {
    pgrep -f "$1" > /dev/null
}

# Step 1: Clean old logs
print_status "🧹 Step 1: Cleaning old log files..." "$BLUE"
rm -f client-sent-wal-batches.log server-received-wal-batches.log
echo "   ✅ Log files cleaned"
echo ""

# Step 2: Compile the project
print_status "🔨 Step 2: Compiling project..." "$BLUE"
mvn compile -q
if [ $? -eq 0 ]; then
    echo "   ✅ Compilation successful"
else
    print_status "   ❌ Compilation failed" "$RED"
    exit 1
fi
echo ""

# Step 3: Generate classpath
print_status "📚 Step 3: Generating classpath..." "$BLUE"
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt -q
if [ -f cp.txt ]; then
    echo "   ✅ Classpath generated"
else
    print_status "   ❌ Classpath generation failed" "$RED"
    exit 1
fi
echo ""

# Step 4: Start the WAL Batch server
print_status "🚀 Step 4: Starting WAL Batch gRPC Server..." "$BLUE"
echo "   Starting server in background..."

# Start server with proper JVM args for Chronicle Queue
java --add-exports java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens java.base/java.nio=ALL-UNNAMED \
     --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
     -cp "target/classes:$(cat cp.txt)" \
     com.telcobright.oltp.example.WALBatchTestServer > server-startup.log 2>&1 &

SERVER_PID=$!
echo "   Server PID: $SERVER_PID"

# Wait for server to start
print_status "   ⏳ Waiting for server to start..." "$YELLOW"
sleep 5

# Check if server is still running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    print_status "   ❌ Server failed to start. Check server-startup.log" "$RED"
    cat server-startup.log
    exit 1
fi

echo "   ✅ Server started successfully"
echo ""

# Step 5: Run payload verification test client
print_status "🧪 Step 5: Running Payload Verification Test Client..." "$BLUE"
java --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
     -cp "target/classes:$(cat cp.txt)" \
     com.telcobright.oltp.example.PayloadVerificationTest

CLIENT_EXIT_CODE=$?

if [ $CLIENT_EXIT_CODE -eq 0 ]; then
    echo "   ✅ Client test completed successfully"
else
    print_status "   ❌ Client test failed with exit code: $CLIENT_EXIT_CODE" "$RED"
fi
echo ""

# Step 6: Stop the server
print_status "🛑 Step 6: Stopping server..." "$BLUE"
kill $SERVER_PID 2>/dev/null
sleep 2

if kill -0 $SERVER_PID 2>/dev/null; then
    print_status "   ⚠️  Server still running, force killing..." "$YELLOW"
    kill -9 $SERVER_PID 2>/dev/null
fi
echo "   ✅ Server stopped"
echo ""

# Step 7: Check log files existence
print_status "📝 Step 7: Checking log files..." "$BLUE"

if [ -f "client-sent-wal-batches.log" ]; then
    CLIENT_ENTRIES=$(wc -l < client-sent-wal-batches.log)
    echo "   ✅ Client log file exists with $CLIENT_ENTRIES entries"
else
    print_status "   ❌ Client log file missing" "$RED"
    exit 1
fi

if [ -f "server-received-wal-batches.log" ]; then
    SERVER_ENTRIES=$(wc -l < server-received-wal-batches.log)
    echo "   ✅ Server log file exists with $SERVER_ENTRIES entries"
else
    print_status "   ❌ Server log file missing" "$RED"
    exit 1
fi
echo ""

# Step 8: Compare payloads
print_status "🔍 Step 8: Comparing Client and Server Payloads..." "$BLUE"
java -cp "target/classes:$(cat cp.txt)" \
     com.telcobright.oltp.example.PayloadLogComparator

COMPARATOR_EXIT_CODE=$?
echo ""

# Final Results
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                        FINAL RESULTS                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"

if [ $CLIENT_EXIT_CODE -eq 0 ] && [ $COMPARATOR_EXIT_CODE -eq 0 ]; then
    print_status "🎉 SUCCESS: All payloads match exactly between client and server!" "$GREEN"
    print_status "✅ WALEntryBatch system working perfectly!" "$GREEN"
    echo ""
    echo "📊 Summary:"
    echo "   • Client sent: $CLIENT_ENTRIES WAL batches"
    echo "   • Server received: $SERVER_ENTRIES WAL batches"
    echo "   • Payload matching: PERFECT ✨"
    echo ""
    echo "📝 Log Files Generated:"
    echo "   • client-sent-wal-batches.log"
    echo "   • server-received-wal-batches.log"
    echo ""
else
    print_status "❌ FAILURE: Issues detected during testing!" "$RED"
    echo ""
    echo "🔧 Debug Information:"
    echo "   • Client exit code: $CLIENT_EXIT_CODE"
    echo "   • Comparator exit code: $COMPARATOR_EXIT_CODE"
    echo "   • Check the log files for details"
fi

# Cleanup
rm -f cp.txt server-startup.log

exit $COMPARATOR_EXIT_CODE