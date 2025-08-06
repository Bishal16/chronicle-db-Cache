package com.telcobright.oltp.config;


public class DebeziumEvent {
    public Schema schema;
    public Payload payload;

    public static class Schema {}

    public static class Payload {
        public PackageAccountCdc before;
        public PackageAccountCdc after;
        public Source source;
        public String op;
    }

    public static class Source {
        public String version;
        public String connector;
        public String name;
        public Long ts_ms;
        public String snapshot;
        public String db;
        public String table;
    }
}

