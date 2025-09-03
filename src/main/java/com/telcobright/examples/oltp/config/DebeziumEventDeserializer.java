package com.telcobright.examples.oltp.config;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class DebeziumEventDeserializer extends ObjectMapperDeserializer<DebeziumEvent> {
    public DebeziumEventDeserializer() {
        super(DebeziumEvent.class);
    }
}

