package com.github.lechandonga.eventtimewindowaggregatorjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end via the real HTTP stack on a random ephemeral port (server.port=0).
 * Covers the business: concurrent ingress yields the same window result as
 * serial processing, plus the query / DLQ / metrics contract.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "etwa.window-size-ms=10000",
                "etwa.allowed-lateness-ms=5000",
                "etwa.queue-capacity=512",
                "etwa.sink-max-attempts=2",
                "etwa.sink-backoff-ms=1"
        })
class HttpApiIntegrationTest {

    private static Path DATA_DIR;

    @BeforeAll
    static void assignDataDir(@TempDir Path tempDir) {
        DATA_DIR = tempDir.resolve("http-data");
        System.setProperty("etwa.data-dir", DATA_DIR.toString());
    }

    @LocalServerPort
    int port;

    private final TestRestTemplate rest = new TestRestTemplate();
    private final ObjectMapper mapper = new ObjectMapper();

    private String url(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    void concurrentIngressMatchesSerialResult() throws Exception {
        int threads = 8;
        int perThread = 25;
        ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        Set<String> ids = ConcurrentHashMap.newKeySet();

        for (int t = 0; t < threads; t++) {
            final int base = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        String id = "ev-" + base + "-" + i;
                        ids.add(id);
                        postEvent(id, "k1", 11_000 + (i % 5_000), 1);
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(20, TimeUnit.SECONDS));
        pool.shutdown();

        // some duplicate redeliveries from a retrying client
        for (int i = 0; i < 10; i++) {
            postEvent("ev-0-" + i, "k1", 11_000 + i, 1);
        }
        postEvent("closer", "k1", 30_000, 0);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        long expectedUnique = ids.size();
        while (System.nanoTime() < deadline) {
            JsonNode metrics = rest.getForObject(url("/metrics"), JsonNode.class);
            if (metrics.path("lastConsumedOffset").asLong() >= expectedUnique + 10L) {
                break;
            }
            Thread.sleep(20);
        }

        JsonNode body = rest.getForObject(url("/results?windowStart=10000&key=k1"), JsonNode.class);
        assertEquals(1, body.path("count").asInt());
        JsonNode window = body.path("results").get(0);
        assertEquals(expectedUnique, window.path("count").asLong(),
                "concurrent ingress + redelivery must equal the unique-id set (serial semantics)");
        assertEquals(expectedUnique, window.path("sum").asLong());
        assertEquals(10, rest.getForObject(url("/metrics"), JsonNode.class).path("duplicate").asLong());
    }

    @Test
    void lateAndMalformedEventsAreQueryableWithReasons() throws Exception {
        // self-contained: close [20000,30000) (and earlier windows) via key k2,
        // independent of test execution order
        postEvent("late-closer", "k2", 50_000, 0);
        postEvent("late-http", "k2", 21_000, 3);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        rest.postForEntity(url("/events"), new HttpEntity<>("{bad json", headers), String.class);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            JsonNode dlq = rest.getForObject(url("/dead-letters"), JsonNode.class);
            boolean hasLate = false;
            boolean hasMalformed = false;
            for (JsonNode d : dlq.path("deadLetters")) {
                hasLate |= "TOO_LATE".equals(d.path("reason").asText());
                hasMalformed |= "MALFORMED".equals(d.path("reason").asText());
            }
            if (hasLate && hasMalformed) {
                break;
            }
            Thread.sleep(20);
        }

        JsonNode lateOnly = rest.getForObject(url("/dead-letters?reason=TOO_LATE"), JsonNode.class);
        for (JsonNode d : lateOnly.path("deadLetters")) {
            assertEquals("TOO_LATE", d.path("reason").asText());
            assertTrue(d.path("detail").asText().contains("already closed"));
        }
        JsonNode health = rest.getForObject(url("/health"), JsonNode.class);
        assertEquals("UP", health.path("status").asText());
    }

    private void postEvent(String id, String key, long eventTime, long value) throws Exception {
        String json = mapper.writeValueAsString(new java.util.LinkedHashMap<>() {{
            put("id", id);
            put("key", key);
            put("eventTime", eventTime);
            put("value", value);
        }});
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        var response = rest.postForEntity(url("/events"), new HttpEntity<>(json, headers), String.class);
        assertTrue(response.getStatusCode().is2xxSuccessful(), "ingestion rejected: " + response.getBody());
    }
}
