package com.github.lechandonga.eventtimewindowaggregatorjava.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetter;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only dead-letter log. Nothing is silently dropped: late events,
 * malformed records and exhausted sink writes all land here with a stable
 * reason code, and are also exposed over HTTP.
 */
public class DeadLetterStore {

    private final Path file;
    private final ObjectMapper mapper;
    private final Object lock = new Object();
    private final List<DeadLetter> inMemory = new ArrayList<>();

    public DeadLetterStore(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("dead-letters.jsonl");
        this.mapper = mapper;
        try {
            Files.createDirectories(dataDir);
            if (Files.exists(file)) {
                try (BufferedReader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (!line.isBlank()) {
                            inMemory.add(mapper.readValue(line, DeadLetter.class));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot initialize dead-letter store", e);
        }
    }

    public DeadLetter record(DeadLetter deadLetter) {
        synchronized (lock) {
            try {
                Files.writeString(file, mapper.writeValueAsString(deadLetter) + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new IllegalStateException("failed to persist dead letter", e);
            }
            inMemory.add(deadLetter);
            return deadLetter;
        }
    }

    public List<DeadLetter> all() {
        synchronized (lock) {
            return List.copyOf(inMemory);
        }
    }

    public Path path() {
        return file;
    }
}
