package com.github.lechandonga.eventtimewindowaggregatorjava.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Incoming business event. {@code eventTime} is the event-time used for
 * windowing (epoch millis); {@code id} provides idempotent de-duplication.
 */
public record Event(
        @JsonProperty("id") String id,
        @JsonProperty("key") String key,
        @JsonProperty("eventTime") long eventTime,
        @JsonProperty("value") long value) {

    public Event {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("event id must not be blank");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("event key must not be blank");
        }
        if (eventTime < 0) {
            throw new IllegalArgumentException("eventTime must be >= 0");
        }
    }
}
