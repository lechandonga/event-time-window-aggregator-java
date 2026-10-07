package com.github.lechandonga/eventtimewindowaggregatorjava/model;

/**
 * 结果写出重试耗尽后进入的死信记录，带可区分原因。
 */
public record DeadLetter(String resultId, long windowStart, long windowEnd, String key,
                         String reason, int attempts, String error, long deadAt) {

    public static final String REASON_SINK_RETRIES_EXHAUSTED = "SINK_RETRIES_EXHAUSTED";
}
