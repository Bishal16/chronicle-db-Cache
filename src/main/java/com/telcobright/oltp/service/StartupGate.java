package com.telcobright.oltp.service;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class StartupGate {
    private final AtomicBoolean replayInProgress = new AtomicBoolean(); // starts as false

    public boolean isReplayInProgress() {
        return replayInProgress.get();
    }

    public void markReplayInProgress() {
        replayInProgress.set(true);
    }

    public void markReplayComplete() {
        replayInProgress.set(false);
    }
}

