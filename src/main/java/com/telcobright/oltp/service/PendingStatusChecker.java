package com.telcobright.oltp.service;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class PendingStatusChecker {
    private final AtomicBoolean replayInProgress = new AtomicBoolean(true);

    public boolean isReplayInProgress() {
        return replayInProgress.get();
    }

    public void markReplayComplete() {
        replayInProgress.set(false);
    }
}

