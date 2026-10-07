package com.github.lechandonga.eventtimewindowaggregatorjava.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Append-only local write-ahead log.
 *
 * <p>Each record occupies one line: {@code <offset>:<base64-json>}. The
 * offset is a strictly increasing sequence number starting at 0 and is
 * assigned atomically with the durable append, so callers know the exact
 * consumer position represented by every record.
 *
 * <p>The file is flushed and fsynced after every accepted record, i.e. an
 * event is acknowledged only once it survives a process kill.
 */
public class WriteAheadLog implements AutoCloseable {

    public record WalRecord(long offset, String rawJson) {
    }

    private final Path file;
    private final ObjectMapper mapper;
    private final Object appendLock = new Object();
    private java.io.RandomAccessFile raf;
    private long nextOffset;

    public WriteAheadLog(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("events.wal.log");
        this.mapper = mapper;
        try {
            Files.createDirectories(dataDir);
            this.raf = new java.io.RandomAccessFile(file.toFile(), "rw");
            this.nextOffset = countLines();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open WAL at " + file, e);
        }
    }

    /** Durably appends one record and returns its assigned offset. */
    public long appendRaw(String rawJson) {
        String encoded = java.util.Base64.getEncoder().encodeToString(rawJson.getBytes(StandardCharsets.UTF_8));
        return appendEncoded(encoded);
    }

    /** Durably appends one POJO, serialized to JSON, and returns its offset. */
    public long append(Object event) {
        byte[] json;
        try {
            json = mapper.writeValueAsBytes(event);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to serialize WAL record", e);
        }
        return appendEncoded(java.util.Base64.getEncoder().encodeToString(json));
    }

    private long appendEncoded(String encoded) {
        byte[] line = (nextOffset + ":" + encoded + "\n").getBytes(StandardCharsets.UTF_8);
        synchronized (appendLock) {
            long offset = nextOffset;
            try {
                raf.seek(raf.length());
                raf.write(line);
                raf.getFD().sync();
            } catch (IOException e) {
                throw new UncheckedIOException("failed to append to WAL", e);
            }
            nextOffset++;
            return offset;
        }
    }

    /** Streams all records with offset &gt;= {@code fromOffset} to the given consumer. */
    public void replay(long fromOffset, java.util.function.Consumer<WalRecord> consumer) throws IOException {
        long lineNo = 0;
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    lineNo++;
                    continue;
                }
                int sep = line.indexOf(':');
                if (sep < 0) {
                    throw new IOException("corrupt WAL line " + lineNo + ": missing offset separator");
                }
                final long storedOffset;
                final String rawJson;
                try {
                    storedOffset = Long.parseLong(line.substring(0, sep));
                    rawJson = new String(java.util.Base64.getDecoder().decode(line.substring(sep + 1)),
                            StandardCharsets.UTF_8);
                } catch (IllegalArgumentException e) {
                    throw new IOException("corrupt WAL line " + lineNo, e);
                }
                if (storedOffset != lineNo) {
                    throw new IOException("corrupt WAL line " + lineNo + ": offset mismatch " + storedOffset);
                }
                if (storedOffset >= fromOffset) {
                    consumer.accept(new WalRecord(storedOffset, rawJson));
                }
                lineNo++;
            }
        }
    }

    /** Number of durable records (also the offset that the next append gets). */
    public long size() {
        return nextOffset;
    }

    public Path path() {
        return file;
    }

    private long countLines() throws IOException {
        long count = 0;
        try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            while (reader.readLine() != null) {
                count++;
            }
        }
        return count;
    }

    @Override
    @PreDestroy
    public void close() {
        synchronized (appendLock) {
            try {
                if (raf != null) {
                    raf.close();
                    raf = null;
                }
            } catch (IOException ignored) {
                // best effort on shutdown
            }
        }
    }
}
