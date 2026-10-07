package com.github.lechandonga.eventtimewindowaggregatorjava.model;

/**
 * 输入事件。eventTime 为事件时间（毫秒，由生产者产生），eventId 全局唯一用于幂等去重。
 */
public record Event(String eventId, long eventTime, String key, double value) {

    public static final String DEFAULT_KEY = "default";

    public Event(String eventId, long eventTime, double value) {
        this(eventId, eventTime, DEFAULT_KEY, value);
    }
}
