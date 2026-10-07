package com.github.lechandonga.eventtimewindowaggregatorjava.agg;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.eventtimewindowaggregatorjava.dlq.DeadLetterStore;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetter;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetterReason;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.Event;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;
import com.github.lechandonga.eventtimewindowaggregatorjava.sink.RetryingSink;
import com.github.lechandonga.eventtimewindowaggregatorjava.state.CheckpointState;
import com.github.lechandonga.eventtimewindowaggregatorjava.state.CheckpointStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Single-threaded event-time window aggregator.
 *
 * <p>All state transitions happen on the one consumer thread (WAL replay
 * runs before it starts), which makes concurrent ingestion equivalent to
 * serial processing: ingress threads only append to the WAL.
 *
 * <p>Semantics (see docs/SEMANTICS.md):
 * <ul>
 *   <li>tumbling windows aligned to multiples of {@code windowSizeMs};</li>
 *   <li>watermark = max(observed eventTime) - allowedLatenessMs;</li>
 *   <li>a window closes once watermark &gt;= windowEnd; its results then
 *       become visible and are written to the sink;</li>
 *   <li>events whose window already closed are dead-lettered as
 *       TOO_LATE and never modify a closed window;</li>
 *   <li>duplicate event ids within a window are counted once.</li>
 * </ul>
 */
public class Aggregator {

    private static final Logger log = LoggerFactory.getLogger(Aggregator.class);

    private final CheckpointState state;
    private final long windowSizeMs;
    private final long allowedLatenessMs;
    private final RetryingSink sink;
    private final DeadLetterStore deadLetters;
    private final CheckpointStore checkpointStore;
    private final ObjectMapper mapper;
    private final ServiceMetrics metrics;

    public Aggregator(CheckpointState state,
                      RetryingSink sink,
                      DeadLetterStore deadLetters,
                      CheckpointStore checkpointStore,
                      ObjectMapper mapper,
                      ServiceMetrics metrics) {
        this.state = state;
        this.windowSizeMs = state.getWindowSizeMs();
        this.allowedLatenessMs = state.getAllowedLatenessMs();
        this.sink = sink;
        this.deadLetters = deadLetters;
        this.checkpointStore = checkpointStore;
        this.mapper = mapper;
        this.metrics = metrics;
    }

    /**
     * Applies one raw WAL record and atomically advances offset + state.
     * Exactly one checkpoint is written per record (offset and aggregate
     * state commit together).
     */
    public synchronized void process(long offset, String rawJson) {
        Event event = null;
        try {
            event = mapper.readValue(rawJson, Event.class);
        } catch (Exception parseError) {
            metrics.malformed.incrementAndGet();
            deadLetters.record(DeadLetter.malformed(offset, abbreviate(rawJson),
                    DeadLetterReason.MALFORMED, parseError.getMessage()));
            log.warn("offset={} dead-lettered as MALFORMED: {}", offset, parseError.getMessage());
        }
        if (event != null) {
            applyEvent(offset, event);
        }
        state.setLastConsumedOffset(offset);
        state.setLastRecordDigest(CheckpointStore.sha256(rawJson.getBytes(StandardCharsets.UTF_8)));
        checkpointStore.save(state);
        metrics.checkpoints.incrementAndGet();
    }

    private void applyEvent(long offset, Event event) {
        long windowStart = floorWindow(event.eventTime());
        long windowEnd = windowStart + windowSizeMs;

        if (isClosed(windowStart, windowEnd)) {
            metrics.late.incrementAndGet();
            String detail = "window [" + windowStart + "," + windowEnd + ") already closed; watermark="
                    + state.getWatermark();
            deadLetters.record(DeadLetter.of(offset, event, DeadLetterReason.TOO_LATE, detail));
            log.info("offset={} eventId={} eventTime={} dead-lettered as TOO_LATE ({})",
                    offset, event.id(), event.eventTime(), detail);
            return;
        }

        Map<String, WindowAccumulator> keys =
                state.getActiveWindows().computeIfAbsent(windowStart, k -> new TreeMap<>());
        WindowAccumulator accumulator = keys.computeIfAbsent(event.key(), k -> new WindowAccumulator(windowStart, k));
        if (!accumulator.add(event)) {
            metrics.duplicate.incrementAndGet();
            log.info("offset={} duplicate eventId={} ignored for window={} key={}",
                    offset, event.id(), windowStart, event.key());
        } else {
            metrics.consumed.incrementAndGet();
        }

        advanceWatermarkAndClose(event.eventTime());
    }

    private boolean isClosed(long windowStart, long windowEnd) {
        if (state.getClosedWindows().containsKey(windowStart)) {
            return true;
        }
        return state.getWatermark() != Long.MIN_VALUE && windowEnd <= state.getWatermark();
    }

    private void advanceWatermarkAndClose(long eventTime) {
        long candidate = eventTime - allowedLatenessMs;
        if (candidate <= state.getWatermark()) {
            return;
        }
        state.setWatermark(candidate);

        List<Long> toClose = new ArrayList<>();
        for (Long start : state.getActiveWindows().keySet()) {
            if (start + windowSizeMs <= state.getWatermark()) {
                toClose.add(start);
            }
        }
        for (Long start : toClose) {
            closeWindow(start);
        }
    }

    private void closeWindow(long start) {
        Map<String, WindowAccumulator> keys = state.getActiveWindows().remove(start);
        if (keys == null) {
            return;
        }
        state.getClosedWindows().put(start, keys);
        for (WindowAccumulator accumulator : keys.values()) {
            WindowResult result = accumulator.toResult(windowSizeMs);
            try {
                sink.write(result);
            } catch (Exception e) {
                metrics.sinkDeadLettered.incrementAndGet();
                DeadLetter deadLetter = new DeadLetter(-1L,
                        "window@" + start, accumulator.getKey(), start, accumulator.getSum(),
                        DeadLetterReason.SINK_RETRY_EXHAUSTED.code(),
                        "closed window result could not be written after " + sink.maxAttempts()
                                + " attempts: " + e.getMessage() + " | eventIds=" + accumulator.getEventsById().keySet(),
                        System.currentTimeMillis());
                deadLetters.record(deadLetter);
                log.error("window={} key={} result dead-lettered as SINK_RETRY_EXHAUSTED: {}",
                        start, accumulator.getKey(), e.getMessage());
            }
        }
        metrics.windowsClosed.incrementAndGet();
        log.info("window [{},{}) closed with {} key(s); results now visible",
                start, start + windowSizeMs, keys.size());
    }

    private long floorWindow(long eventTime) {
        return Math.floorDiv(eventTime, windowSizeMs) * windowSizeMs;
    }

    private static String abbreviate(String raw) {
        return raw.length() <= 200 ? raw : raw.substring(0, 200) + "...";
    }

    public synchronized long watermark() {
        return state.getWatermark();
    }

    /** Only closed windows are returned: open-window aggregates stay invisible. */
    public synchronized List<WindowResult> visibleResults(Long windowStart, String key) {
        List<WindowResult> results = new ArrayList<>();
        for (Map.Entry<Long, Map<String, WindowAccumulator>> entry :
                new TreeMap<>(state.getClosedWindows()).entrySet()) {
            if (windowStart != null && entry.getKey() != windowStart.longValue()) {
                continue;
            }
            for (WindowAccumulator accumulator : new TreeMap<>(entry.getValue()).values()) {
                if (key != null && !key.equals(accumulator.getKey())) {
                    continue;
                }
                results.add(accumulator.toResult(windowSizeMs));
            }
        }
        return results;
    }

    public synchronized List<WindowResult> visibleResults() {
        return visibleResults(null, null);
    }
}
