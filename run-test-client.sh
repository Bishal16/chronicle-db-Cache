#!/bin/bash

echo "Starting Test Cases gRPC Client..."
echo "=================================="

mvn exec:java -Dexec.mainClass="com.telcobright.oltp.example.TestCasesGrpcClientLauncher" -Dexec.args="localhost 9000" -Dexec.classpathScope=compile -Dorg.slf4j.simpleLogger.defaultLogLevel=info -q