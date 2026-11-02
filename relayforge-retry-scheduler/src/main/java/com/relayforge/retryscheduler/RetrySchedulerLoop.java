package com.relayforge.retryscheduler;

import java.util.Objects;
import org.springframework.scheduling.annotation.Scheduled;

public final class RetrySchedulerLoop {
    private final RetrySchedulerService scheduler;

    public RetrySchedulerLoop(RetrySchedulerService scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler);
    }

    @Scheduled(fixedDelay = 1000)
    public void scan() {
        scheduler.scanDue(100);
    }
}
