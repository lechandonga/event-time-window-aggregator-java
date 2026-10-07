package com.github.lechandonga.eventtimewindowaggregatorjava.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A record routed to the dead-letter path. {@code reason} is one of the
 * stable codes in {@link DeadLetterReason}.
 */
public record DeadLetter(
        @JsonProperty("offset") long offset,
        @JsonProperty("eventId") String eventId,
        @JsonProperty("key") String key,
        @JsonProperty("eventTime") long eventTime,
        @JsonProperty("value") long value,
        @JsonProperty("reason") String reason,
        @JsonProperty("detail") String detail,
        @JsonProperty("deadLetteredAt") long deadLetteredAt) {

    public static DeadLetter of(long offset, Event event, DeadLetterReason reason, String detail) {
        return new DeadLetter(offset, event.id(), event.key(), event.eventTime(), event.value(),
                reason.code(), detail, System.currentTimeMillis());
    }

    public static DeadLetter malformed(long offset, String raw, DeadLetterReason reason, String detail) {
        return new DeadLetter(offset, null, null, -1L, 0L, reason.code(),
                "raw=" + raw + " | " + detail, System.currentTimeMillis());
    }
}
