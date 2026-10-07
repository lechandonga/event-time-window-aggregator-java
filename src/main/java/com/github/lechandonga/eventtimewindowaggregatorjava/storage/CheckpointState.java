package com.github.lechandonga.eventtimewindowaggregatorjava.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetter;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.LateEvent;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 与 WAL 位点原子落盘的完整聚合状态快照。位点与状态必须一致，缺一不可恢复。
 */
public class CheckpointState {

    /** 检查点格式版本。 */
    public static final int FORMAT_VERSION = 1;

    @JsonProperty("version")
    public int version = FORMAT_VERSION;

    /** 下一条待消费的 WAL 序列号（0 起）。 */
    @JsonProperty("nextSeq")
    public long nextSeq;

    /** 已提交前缀的 CRC32（按原始帧字节计算）。 */
    @JsonProperty("logDigest")
    public long logDigest;

    /** 最近观测到的最大事件时间，用于恢复水位线。 */
    @JsonProperty("maxEventTime")
    public long maxEventTime = Long.MIN_VALUE;

    /** 已见过的事件 ID（幂等去重）。 */
    @JsonProperty("seenIds")
    public Set<String> seenIds = new LinkedHashSet<>();

    /** 未关闭窗口状态：windowStart -> key -> [count,sum]。 */
    @JsonProperty("openWindows")
    public Map<Long, Map<String, long[]>> openWindows = new LinkedHashMap<>();

    /** 已关闭并对外可见的窗口结果。 */
    @JsonProperty("closedResults")
    public List<WindowResult> closedResults = new ArrayList<>();

    /** 已写出（含进入死信）的结果 ID，保证结果写出的幂等性。 */
    @JsonProperty("emittedResultIds")
    public Set<String> emittedResultIds = new LinkedHashSet<>();

    /** 超容忍迟到事件。 */
    @JsonProperty("lateEvents")
    public List<LateEvent> lateEvents = new ArrayList<>();

    /** 结果写出死信。 */
    @JsonProperty("deadLetters")
    public List<DeadLetter> deadLetters = new ArrayList<>();

    @JsonProperty("checkpointAt")
    public long checkpointAt;
}
