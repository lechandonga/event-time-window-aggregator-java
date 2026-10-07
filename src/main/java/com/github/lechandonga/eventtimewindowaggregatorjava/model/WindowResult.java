package com.github.lechandonga.eventtimewindowaggregatorjava.model;

/**
 * 一个 [windowStart, windowEnd) 窗口内、单个 key 的聚合结果。窗口关闭后才会生成。
 */
public record WindowResult(long windowStart, long windowEnd, String key, long count, double sum) {

    public String resultId() {
        return windowStart + ":" + key;
    }
}
