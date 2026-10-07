package com.github.lechandonga.eventtimewindowaggregatorjava.state;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.lechandonga.eventtimewindowaggregatorjava.agg.WindowAccumulator;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Full, self-contained snapshot of consumer position + aggregation state.
 * Offset and state always move together: they are persisted in one atomic
 * file, so a crash can never commit a position without its state or vice
 * versa.
 */
public class CheckpointState {

    public static final int FORMAT_VERSION = 1;

    private int version = FORMAT_VERSION;
    private long windowSizeMs;
    private long allowedLatenessMs;
    /** Offset of the last fully processed WAL record; -1 when nothing processed. */
    private long lastConsumedOffset = -1;
    /** SHA-256 hex of the last processed record's raw JSON line, for cross-checking the WAL. */
    private String lastRecordDigest;
    /** Current event-time watermark (epoch millis); Long.MIN_VALUE before any event. */
    private long watermark = Long.MIN_VALUE;
    private Map<Long, Map<String, WindowAccumulator>> activeWindows = new LinkedHashMap<>();
    /** Closed windows remain retained (results are immutable and still queryable). */
    private Map<Long, Map<String, WindowAccumulator>> closedWindows = new LinkedHashMap<>();

    public CheckpointState() {
    }

    public CheckpointState(long windowSizeMs, long allowedLatenessMs) {
        this.windowSizeMs = windowSizeMs;
        this.allowedLatenessMs = allowedLatenessMs;
    }

    @JsonProperty public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    @JsonProperty public long getWindowSizeMs() { return windowSizeMs; }
    public void setWindowSizeMs(long windowSizeMs) { this.windowSizeMs = windowSizeMs; }
    @JsonProperty public long getAllowedLatenessMs() { return allowedLatenessMs; }
    public void setAllowedLatenessMs(long allowedLatenessMs) { this.allowedLatenessMs = allowedLatenessMs; }
    @JsonProperty public long getLastConsumedOffset() { return lastConsumedOffset; }
    public void setLastConsumedOffset(long lastConsumedOffset) { this.lastConsumedOffset = lastConsumedOffset; }
    @JsonProperty public String getLastRecordDigest() { return lastRecordDigest; }
    public void setLastRecordDigest(String lastRecordDigest) { this.lastRecordDigest = lastRecordDigest; }
    @JsonProperty public long getWatermark() { return watermark; }
    public void setWatermark(long watermark) { this.watermark = watermark; }
    @JsonProperty public Map<Long, Map<String, WindowAccumulator>> getActiveWindows() { return activeWindows; }
    public void setActiveWindows(Map<Long, Map<String, WindowAccumulator>> activeWindows) { this.activeWindows = activeWindows; }
    @JsonProperty public Map<Long, Map<String, WindowAccumulator>> getClosedWindows() { return closedWindows; }
    public void setClosedWindows(Map<Long, Map<String, WindowAccumulator>> closedWindows) { this.closedWindows = closedWindows; }
}
