package com.telcobright.oltp.queue.chronicle;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConsumerToQueue<T> extends Runnable {
//    void execute(T message);
    void updatePackageAccountTable(T message, Connection connection) throws SQLException;
    void shutdown();
}