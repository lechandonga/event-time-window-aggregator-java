package com.github.lechandonga.eventtimewindowaggregatorjava.agg;

import java.util.concurrent.atomic.AtomicLong;

/** Plain counters describing the processing pipeline; exposed via /metrics. */
public class ServiceMetrics {

    public final AtomicLong ingested = new AtomicLong();
    public final AtomicLong consumed = new AtomicLong();
    public final AtomicLong duplicate = new AtomicLong();
    public final AtomicLong late = new AtomicLong();
    public final AtomicLong malformed = new AtomicLong();
    public final AtomicLong sinkRetried = new AtomicLong();
    public final AtomicLong sinkDeadLettered = new AtomicLong();
    public final AtomicLong windowsClosed = new AtomicLong();
    public final AtomicLong rejectedQueueFull = new AtomicLong();
    public final AtomicLong checkpoints = new AtomicLong();
}
