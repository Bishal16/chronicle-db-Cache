package com.telcobright.oltp.service;

import com.telcobright.oltp.queue.chronicle.ChronicleInstance;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import net.openhft.chronicle.queue.RollCycle;
import net.openhft.chronicle.queue.RollCycles;
import org.eclipse.microprofile.config.inject.ConfigProperty;

// @Startup  // Temporarily disabled for testing gRPC
@ApplicationScoped
public class ChronicleInstanceFactory {

    private final ChronicleInstance chronicleInstance;

    @Inject
    public ChronicleInstanceFactory(
            @ConfigProperty(name = "chronicle.queue.path") String queuePath,
            @ConfigProperty(name = "chronicle.queue.roll-cycle", defaultValue = "DAILY") String rollCycleName
    ) {
        // Temporarily disabled for testing gRPC
        // RollCycle rollCycle = mapRollCycle(rollCycleName);
        // this.chronicleInstance = new ChronicleInstance(queuePath, rollCycle);
        this.chronicleInstance = null;
    }

    @Produces
    @ApplicationScoped
    public ChronicleInstance produceChronicleInstance() {
        return chronicleInstance;
    }

    private RollCycle mapRollCycle(String rollCycleName) {
        return switch (rollCycleName.toUpperCase()) {
            case "DAILY" -> RollCycles.DAILY;
            case "HOURLY" -> RollCycles.HOURLY;
            case "LARGE_HOURLY" -> RollCycles.LARGE_HOURLY;
            default -> RollCycles.DAILY;
        };
    }

    @PreDestroy
    public void shutdown() {
        chronicleInstance.shutdown();
    }
}
