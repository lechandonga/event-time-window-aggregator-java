package com.github.lechandonga.eventtimewindowaggregatorjava.proc;

import com.github.lechandonga.eventtimewindowaggregatorjava.wal.WriteAheadLog;

/** A durable WAL record waiting on the consumer thread. */
public record PendingEvent(long offset, String rawJson) {
    public static PendingEvent from(WriteAheadLog.WalRecord record) {
        return new PendingEvent(record.offset(), record.rawJson());
    }
}
