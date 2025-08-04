package com.telcobright.oltp.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;

@ApplicationScoped
public class KafkaConfigListener {

    @Incoming("config-event-loader")
    public void onMessage(String message) {
        System.out.println("🔥 Received Kafka message: " + message);
        // reloadConfigurations(); // Call your config loader here
    }
}
