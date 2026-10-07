package com.github.lechandonga.eventtimewindowaggregatorjava.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Predicate;

/**
 * Local file sink: appends one JSON line per closed-window result.
 *
 * <p>An optional failure trigger (key predicate) exists to simulate output
 * failures in tests; production wiring leaves it unset.
 */
public class FileResultSink implements ResultSink {

    private final Path file;
    private final ObjectMapper mapper;
    private volatile Predicate<String> failureTrigger;

    public FileResultSink(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("results.jsonl");
        this.mapper = mapper;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new IllegalStateException("cannot create data dir", e);
        }
    }

    public void setFailureTrigger(Predicate<String> failureTrigger) {
        this.failureTrigger = failureTrigger;
    }

    @Override
    public synchronized void write(WindowResult result) throws IOException {
        Predicate<String> trigger = failureTrigger;
        if (trigger != null && trigger.test(result.key())) {
            throw new IOException("simulated sink failure for key=" + result.key());
        }
        String line = mapper.writeValueAsString(result) + System.lineSeparator();
        Files.writeString(file, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public Path path() {
        return file;
    }
}
