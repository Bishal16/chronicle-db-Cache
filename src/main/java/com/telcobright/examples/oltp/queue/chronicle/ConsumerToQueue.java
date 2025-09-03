package com.telcobright.examples.oltp.queue.chronicle;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConsumerToQueue<T> extends Runnable {
    void updatePackageAccountTable(T message, Connection connection) throws SQLException;
    void shutdown();
}