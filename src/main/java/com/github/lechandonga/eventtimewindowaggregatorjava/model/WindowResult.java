package com.github.lechandonga.eventtimewindowaggregatorjava.model;

import java.util.Map;

/**
 * Immutable aggregation result for one (windowStart, key) pair.
 * A result becomes externally visible only after the window is closed.
 */
public record WindowResult(
        long windowStart,
        long windowEnd,
        String key,
        long count,
        long sum,
        long firstEventTime,
        long lastEventTime,
        Map<String, Long> eventsById) {
}
