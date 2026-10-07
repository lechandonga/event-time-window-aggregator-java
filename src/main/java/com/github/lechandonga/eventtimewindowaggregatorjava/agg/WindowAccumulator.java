package com.github.lechandonga.eventtimewindowaggregatorjava.agg;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.Event;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mutable per-(window, key) aggregate. Remembers every contributing event
 * id so that redelivered events (at-least-once replay after crash, or the
 * client retrying a timed-out ingestion) are counted exactly once.
 */
public class WindowAccumulator {

    private long windowStart;
    private String key;
    private long count;
    private long sum;
    private long firstEventTime = Long.MAX_VALUE;
    private long lastEventTime = Long.MIN_VALUE;
    private final Map<String, Long> eventsById = new LinkedHashMap<>();

    public WindowAccumulator() {
    }

    public WindowAccumulator(long windowStart, String key) {
        this.windowStart = windowStart;
        this.key = key;
    }

    /**
     * Applies an event. Returns false when the event id is already part of
     * this window (duplicate delivery) and is therefore ignored.
     */
    public boolean add(Event event) {
        if (eventsById.putIfAbsent(event.id(), event.eventTime()) != null) {
            return false;
        }
        count++;
        sum += event.value();
        firstEventTime = Math.min(firstEventTime, event.eventTime());
        lastEventTime = Math.max(lastEventTime, event.eventTime());
        return true;
    }

    public WindowResult toResult(long windowSizeMs) {
        return new WindowResult(windowStart, windowStart + windowSizeMs, key, count, sum,
                count == 0 ? windowStart : firstEventTime,
                count == 0 ? windowStart : lastEventTime,
                java.util.Collections.unmodifiableMap(new java.util.TreeMap<>(eventsById)));
    }

    @JsonProperty public long getWindowStart() { return windowStart; }
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
    @JsonProperty public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }
    @JsonProperty public long getCount() { return count; }
    public void setCount(long count) { this.count = count; }
    @JsonProperty public long getSum() { return sum; }
    public void setSum(long sum) { this.sum = sum; }
    @JsonProperty public long getFirstEventTime() { return firstEventTime; }
    public void setFirstEventTime(long firstEventTime) { this.firstEventTime = firstEventTime; }
    @JsonProperty public long getLastEventTime() { return lastEventTime; }
    public void setLastEventTime(long lastEventTime) { this.lastEventTime = lastEventTime; }
    @JsonProperty public Map<String, Long> getEventsById() { return eventsById; }
}
