#!/bin/bash

echo "Starting gRPC Builder Test Client..."
echo "===================================="

# Run the client using Maven with proper logging configuration
mvn exec:java \
  -Dexec.mainClass="com.telcobright.oltp.example.GrpcBuilderTestClientLauncher" \
  -Dexec.args="localhost 9000" \
  -Dexec.classpathScope=compile \
  -Dorg.slf4j.simpleLogger.defaultLogLevel=info \
  -q