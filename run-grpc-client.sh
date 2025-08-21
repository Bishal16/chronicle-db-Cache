#!/bin/bash

echo "Starting gRPC Batch CRUD Client..."
echo "=================================="

# Run the client using Maven with proper logging configuration
mvn exec:java \
  -Dexec.mainClass="com.telcobright.oltp.example.BatchCrudGrpcClientLauncher" \
  -Dexec.args="localhost 9000" \
  -Dexec.classpathScope=compile \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
  -q