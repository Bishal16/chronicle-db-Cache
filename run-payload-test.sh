#!/bin/bash

echo "Starting Payload Verification Test..."
echo "====================================="

# Run the payload test client
mvn exec:java \
  -Dexec.mainClass="com.telcobright.oltp.example.PayloadTestClientLauncher" \
  -Dexec.args="localhost 9000" \
  -Dexec.classpathScope=compile \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
  -q