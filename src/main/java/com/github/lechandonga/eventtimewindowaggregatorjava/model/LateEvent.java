package com.github.lechandonga/eventtimewindowaggregatorjava/model;

/**
 * 超过水位线容忍范围的迟到事件。不静默丢弃：记录原因与当时水位线，单独查询。
 */
public record LateEvent(String eventId, long eventTime, String key, double value, long watermark,
                        long lastClosedWindowEnd, String reason, String detail, long receivedAt) {

    public static final String REASON_TOO_LATE = "AFTER_WATERMARK_TOO_LATE";
}
