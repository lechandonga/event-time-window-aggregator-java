package com.github.lechandonga.eventtimewindowaggregatorjava.sink;

import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded retry wrapper around a {@link ResultSink}. Failures are retried
 * up to {@code maxAttempts} times with a fixed backoff; the final failure
 * propagates so the caller can route the result to the dead-letter path
 * with reason SINK_RETRY_EXHAUSTED.
 */
public class RetryingSink implements ResultSink {

    private static final Logger log = LoggerFactory.getLogger(RetryingSink.class);

    private final ResultSink delegate;
    private final int maxAttempts;
    private final long backoffMs;

    public RetryingSink(ResultSink delegate, int maxAttempts, long backoffMs) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.delegate = delegate;
        this.maxAttempts = maxAttempts;
        this.backoffMs = backoffMs;
    }

    @Override
    public void write(WindowResult result) throws Exception {
        Exception last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                delegate.write(result);
                if (attempt > 1) {
                    log.info("sink write recovered on attempt {}/{} for window={} key={}",
                            attempt, maxAttempts, result.windowStart(), result.key());
                }
                return;
            } catch (Exception e) {
                last = e;
                log.warn("sink write failed attempt {}/{} for window={} key={}: {}",
                        attempt, maxAttempts, result.windowStart(), result.key(), e.getMessage());
                if (attempt < maxAttempts) {
                    Thread.sleep(backoffMs);
                }
            }
        }
        throw last;
    }

    public int maxAttempts() {
        return maxAttempts;
    }
}
