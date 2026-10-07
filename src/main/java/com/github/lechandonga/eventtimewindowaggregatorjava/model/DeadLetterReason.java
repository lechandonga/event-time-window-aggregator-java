package com.github.lechandonga.eventtimewindowaggregatorjava.model;

/**
 * Distinguishable reasons an event can end up on the dead-letter path.
 */
public enum DeadLetterReason {
    /** Event arrived after its window had already closed (watermark > windowEnd + allowedLateness). */
    TOO_LATE("TOO_LATE"),
    /** Event payload could not be parsed/validated. */
    MALFORMED("MALFORMED"),
    /** Result sink kept failing until all retries were exhausted. */
    SINK_RETRY_EXHAUSTED("SINK_RETRY_EXHAUSTED");

    private final String code;

    DeadLetterReason(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }
}
