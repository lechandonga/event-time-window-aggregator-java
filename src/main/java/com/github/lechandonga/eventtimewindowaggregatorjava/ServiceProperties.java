package com.github.lechandonga.eventtimewindowaggregatorjava;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Tunable service parameters. All paths default to a local working directory
 * so the service runs fully standalone without external dependencies.
 */
@ConfigurationProperties(prefix = "etwa")
public class ServiceProperties {

    /** Root directory for the WAL, checkpoint, results and dead-letter files. */
    private String dataDir = "./data";

    /** Tumbling window size in milliseconds (event time). */
    private long windowSizeMs = 10_000;

    /** Allowed lateness in milliseconds measured from the window end. */
    private long allowedLatenessMs = 5_000;

    /**
     * Maximum number of events that may sit in the in-memory processing
     * queue awaiting the consumer. When full the ingress rejects with 503
     * instead of blocking the HTTP thread indefinitely.
     */
    private int queueCapacity = 1_000;

    /** Number of sink attempts (including the first one) before dead-lettering. */
    private int sinkMaxAttempts = 3;

    /** Backoff between sink attempts in milliseconds. */
    private long sinkBackoffMs = 50;

    /** Poll interval of the consumer loop when the queue is idle. */
    private long pollIdleMs = 5;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }
    public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long windowSizeMs) { this.windowSizeMs = windowSizeMs; }
    public long getAllowedLatenessMs() { return allowedLatenessMs; }
    public void setAllowedLatenessMs(long allowedLatenessMs) { this.allowedLatenessMs = allowedLatenessMs; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public int getSinkMaxAttempts() { return sinkMaxAttempts; }
    public void setSinkMaxAttempts(int sinkMaxAttempts) { this.sinkMaxAttempts = sinkMaxAttempts; }
    public long getSinkBackoffMs() { return sinkBackoffMs; }
    public void setSinkBackoffMs(long sinkBackoffMs) { this.sinkBackoffMs = sinkBackoffMs; }
    public long getPollIdleMs() { return pollIdleMs; }
    public void setPollIdleMs(long pollIdleMs) { this.pollIdleMs = pollIdleMs; }
}
