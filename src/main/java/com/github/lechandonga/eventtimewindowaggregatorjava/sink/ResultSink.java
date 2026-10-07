package com.github.lechandonga.eventtimewindowaggregatorjava.sink;

import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;

/**
 * Output path for closed-window results. Implementations must be safe to
 * retry: a failed call may be invoked again with the same result.
 */
@FunctionalInterface
public interface ResultSink {
    void write(WindowResult result) throws Exception;
}
