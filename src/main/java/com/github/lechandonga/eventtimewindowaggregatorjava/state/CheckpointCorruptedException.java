package com.github.lechandonga.eventtimewindowaggregatorjava.state;

/**
 * Raised when the checkpoint is missing (despite an existing WAL),
 * unreadable, checksum-corrupted, or inconsistent with the WAL/config.
 * Recovery is deliberately refused: the service never silently resumes
 * from an unknown intermediate state.
 */
public class CheckpointCorruptedException extends RuntimeException {
    public CheckpointCorruptedException(String message) {
        super(message);
    }

    public CheckpointCorruptedException(String message, Throwable cause) {
        super(message, cause);
    }
}
