package com.github.lechandonga.eventtimewindowaggregatorjava.tests.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.eventtimewindowaggregatorjava.ServiceProperties;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.Event;
import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.time.Duration;

@ExtendWith(ScenarioLogger.class)
public abstract class AbstractScenarioTest {

    protected static final ObjectMapper MAPPER = new ObjectMapper();

    protected ServiceProperties props(Path dataDir) {
        ServiceProperties props = new ServiceProperties();
        props.setDataDir(dataDir.toString());
        props.setWindowSizeMs(10_000);
        props.setAllowedLatenessMs(5_000);
        props.setQueueCapacity(8);
        props.setSinkMaxAttempts(3);
        props.setSinkBackoffMs(5);
        props.setPollIdleMs(2);
        return props;
    }

    protected ProcessingService startService(Path dataDir) {
        ProcessingService service = new ProcessingService(props(dataDir), MAPPER);
        service.start();
        return service;
    }

    protected String json(String id, String key, long eventTime, long value) {
        try {
            return MAPPER.writeValueAsString(new Event(id, key, eventTime, value));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected ProcessingService.IngestResult post(ProcessingService service,
                                                  String id, String key, long eventTime, long value) {
        return service.ingest(json(id, key, eventTime, value));
    }

    protected void awaitDrained(ProcessingService service, long totalWalRecords) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (service.lastConsumedOffset() < totalWalRecords - 1 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        if (service.lastConsumedOffset() < totalWalRecords - 1) {
            throw new AssertionError("consumer did not drain: lastConsumedOffset="
                    + service.lastConsumedOffset() + " expected=" + (totalWalRecords - 1));
        }
    }
}
