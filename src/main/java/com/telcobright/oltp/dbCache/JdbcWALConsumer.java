//package com.telcobright.oltp.dbCache;
//
//import net.openhft.chronicle.queue.ChronicleQueue;
//import net.openhft.chronicle.queue.ExcerptTailer;
//import net.openhft.chronicle.wire.DocumentContext;
//import net.openhft.chronicle.wire.WireIn;
//
//import java.sql.Connection;
//import java.sql.SQLException;
//
//public abstract class JdbcWALConsumer<T> {
//
//    protected final ChronicleQueue queue;
//    protected final Connection dbConnection;
//
//    public JdbcWALConsumer(ChronicleQueue queue, Connection dbConnection) {
//        this.queue = queue;
//        this.dbConnection = dbConnection;
//    }
//
//    public void processNext() throws SQLException {
//        ExcerptTailer tailer = queue.createTailer();
//
//        while (true) {
//            try (DocumentContext dc = tailer.readingDocument()) {
//                if (!dc.isPresent()) {
//                    break;  // No more documents, exit loop
//                }
//
//                WireIn r = dc.wire();
//                int actionOrdinal = r.read("action").int32();
//                CrudActionType action = CrudActionType.values()[actionOrdinal];
//
//                switch (action) {
//                    case Insert -> handleInsert(r, dbConnection);
//                    case Update -> handleUpdate(r, dbConnection);
//                    case Delete -> handleDelete(r, dbConnection);
//                    default -> throw new IllegalStateException("Unknown action type: " + action);
//                }
//            }
//        }
//    }
//
//    protected abstract void handleInsert(WireIn r, Connection dbConnection) throws SQLException;
//
//    protected abstract void handleUpdate(WireIn r, Connection dbConnection) throws SQLException;
//
//    protected abstract void handleDelete(WireIn r, Connection dbConnection) throws SQLException;
//}
