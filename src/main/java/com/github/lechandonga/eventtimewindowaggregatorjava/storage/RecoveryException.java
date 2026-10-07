package com.github.lechandonga.eventtimewindowaggregatorjava.storage;

/**
 * 位点缺失、损坏或与聚合状态/日志对不上时抛出。恢复必须显式失败，拒绝从中间状态启动。
 */
public class RecoveryException extends RuntimeException {

    public RecoveryException(String message) {
        super(message);
    }

    public RecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
