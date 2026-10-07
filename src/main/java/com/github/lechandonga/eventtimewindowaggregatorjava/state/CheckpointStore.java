package com.github.lechandonga.eventtimewindowaggregatorjava.state;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;

/**
 * Persists {@link CheckpointState} as a single file replaced atomically
 * (temp file + fsync + atomic move + directory fsync).
 *
 * <p>File layout: the first line is a small JSON envelope
 * {@code {"sha256":"..."}} and the remainder is the exact state JSON whose
 * SHA-256 the envelope carries. Because the checksum covers the stored byte
 * sequence itself, any torn write, truncation or tampering is detected on
 * load instead of being partially applied.
 */
public class CheckpointStore {

    private final Path file;
    private final Path tempFile;
    private final ObjectMapper mapper;

    public CheckpointStore(Path dataDir, ObjectMapper mapper) {
        this.file = dataDir.resolve("checkpoint.json");
        this.tempFile = dataDir.resolve("checkpoint.json.tmp");
        this.mapper = mapper;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create data dir " + dataDir, e);
        }
    }

    public boolean exists() {
        return Files.exists(file);
    }

    public void save(CheckpointState state) {
        try {
            byte[] payload = mapper.writeValueAsBytes(state);
            byte[] envelope = (mapper.writeValueAsString(Map.of("sha256", sha256(payload)))
                    + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
            byte[] out = new byte[envelope.length + payload.length];
            System.arraycopy(envelope, 0, out, 0, envelope.length);
            System.arraycopy(payload, 0, out, envelope.length, payload.length);

            Files.write(tempFile, out,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            try (var ch = java.nio.channels.FileChannel.open(tempFile, StandardOpenOption.READ)) {
                ch.force(true);
            }
            Files.move(tempFile, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            try (var dirCh = java.nio.channels.FileChannel.open(file.getParent())) {
                dirCh.force(true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to atomically write checkpoint " + file, e);
        }
    }

    public CheckpointState load() {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(file);
        } catch (IOException e) {
            throw new CheckpointCorruptedException("checkpoint unreadable: " + file, e);
        }

        int sep = indexOfNewline(bytes);
        if (sep <= 0) {
            throw new CheckpointCorruptedException("checkpoint envelope missing: " + file);
        }
        final Map<String, String> envelope;
        try {
            envelope = mapper.readValue(bytes, 0, sep, new TypeReference<Map<String, String>>() {
            });
        } catch (IOException e) {
            throw new CheckpointCorruptedException("checkpoint envelope is corrupt: " + file, e);
        }
        String expectedSha = envelope.get("sha256");
        if (expectedSha == null) {
            throw new CheckpointCorruptedException("checkpoint envelope has no sha256: " + file);
        }

        byte[] payload = new byte[bytes.length - sep - 1];
        System.arraycopy(bytes, sep + 1, payload, 0, payload.length);
        if (!expectedSha.equals(sha256(payload))) {
            throw new CheckpointCorruptedException(
                    "checkpoint checksum mismatch: file has been truncated or tampered with: " + file);
        }
        try {
            CheckpointState state = mapper.readValue(payload, CheckpointState.class);
            if (state == null) {
                throw new CheckpointCorruptedException("checkpoint state is empty: " + file);
            }
            return state;
        } catch (IOException e) {
            throw new CheckpointCorruptedException("checkpoint state JSON is corrupt: " + file, e);
        }
    }

    private static int indexOfNewline(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '\n') {
                return i;
            }
        }
        return -1;
    }

    public static String sha256(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public Path path() {
        return file;
    }
}
