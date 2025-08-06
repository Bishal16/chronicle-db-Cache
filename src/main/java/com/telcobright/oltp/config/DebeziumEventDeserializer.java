package com.telcobright.oltp.config;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class DebeziumEventDeserializer extends ObjectMapperDeserializer<DebeziumEvent> {
    public DebeziumEventDeserializer() {
        super(DebeziumEvent.class);
    }
}

