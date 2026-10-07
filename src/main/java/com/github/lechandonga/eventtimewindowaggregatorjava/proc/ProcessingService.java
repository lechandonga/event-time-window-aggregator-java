package com.github.lechandonga.eventtimewindowaggregatorjava.proc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.eventtimewindowaggregatorjava.ServiceProperties;
import com.github.lechandonga.eventtimewindowaggregatorjava.agg.Aggregator;
import com.github.lechandonga.eventtimewindowaggregatorjava.agg.ServiceMetrics;
import com.github.lechandonga.eventtimewindowaggregatorjava.dlq.DeadLetterStore;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;
import com.github.lechandonga.eventtimewindowaggregatorjava.queue.BoundedEventQueue;
import com.github.lechandonga.eventtimewindowaggregatorjava.sink.FileResultSink;
import com.github.lechandonga.eventtimewindowaggregatorjava.sink.RetryingSink;
import com.github.lechandonga.eventtimewindowaggregatorjava.state.CheckpointCorruptedException;
import com.github.lechandonga.eventtimewindowaggregatorjava.state.CheckpointState;
import com.github.lechandonga.eventtimewindowaggregatorjava.state.CheckpointStore;
import com.github.lechandonga.eventtimewindowaggregatorjava.wal.WriteAheadLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the processing pipeline: WAL -&gt; bounded queue -&gt; single consumer
 * thread -&gt; {@link Aggregator} -&gt; atomic checkpoint.
 *
 * <p>On startup the checkpoint is strictly validated against the WAL; any
 * missing/corrupt/inconsistent checkpoint aborts startup rather than
 * resuming from an unknown state.
 */
public class ProcessingService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ProcessingService.class);

    private final ServiceProperties props;
    private final ObjectMapper mapper;

    private WriteAheadLog wal;
    private CheckpointStore checkpointStore;
    private DeadLetterStore deadLetterStore;
    private FileResultSink fileSink;
    private BoundedEventQueue<PendingEvent> queue;
    private Aggregator aggregator;
    private ServiceMetrics metrics;
    private CheckpointState state;

    private Thread consumerThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ProcessingService(ServiceProperties props, ObjectMapper mapper) {
        this.props = props;
        this.mapper = mapper;
    }

    @Override
    public synchronized void start() {
        if (running.get()) {
            return;
        }
        Path dataDir = Path.of(props.getDataDir());
        wal = new WriteAheadLog(dataDir, mapper);
        checkpointStore = new CheckpointStore(dataDir, mapper);
        deadLetterStore = new DeadLetterStore(dataDir, mapper);
        fileSink = new FileResultSink(dataDir, mapper);
        metrics = new ServiceMetrics();

        RetryingSink sink = new RetryingSink(fileSink, props.getSinkMaxAttempts(), props.getSinkBackoffMs()) {
            @Override
            public void write(WindowResult result) throws Exception {
                try {
                    super.write(result);
                } catch (Exception e) {
                    metrics.sinkRetried.addAndGet(Math.max(0, maxAttempts() - 1));
                    throw e;
                }
            }
        };

        state = recoverOrInit(dataDir);
        aggregator = new Aggregator(state, sink, deadLetterStore, checkpointStore, mapper, metrics);
        queue = new BoundedEventQueue<>(props.getQueueCapacity());

        replayUnconsumed();

        running.set(true);
        consumerThread = new Thread(this::consumeLoop, "etwa-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("processing service started (watermark={}, lastConsumedOffset={}, queueCapacity={})",
                state.getWatermark(), state.getLastConsumedOffset(), props.getQueueCapacity());
    }

    private CheckpointState recoverOrInit(Path dataDir) {
        long walRecords = wal.size();
        boolean walHasData = walRecords > 0 || Files.exists(wal.path());

        if (!checkpointStore.exists()) {
            if (walHasData && walRecords > 0) {
                throw new CheckpointCorruptedException(
                        "refuse to start: WAL with " + walRecords + " record(s) exists at " + wal.path()
                                + " but checkpoint " + checkpointStore.path() + " is missing. "
                                + "Delete the data directory explicitly to start from an empty state.");
            }
            log.warn("no checkpoint found and WAL is empty: initializing fresh state under {}", dataDir);
            return new CheckpointState(props.getWindowSizeMs(), props.getAllowedLatenessMs());
        }

        CheckpointState loaded = checkpointStore.load();
        if (loaded.getVersion() != CheckpointState.FORMAT_VERSION) {
            throw new CheckpointCorruptedException(
                    "unsupported checkpoint version " + loaded.getVersion()
                            + ", expected " + CheckpointState.FORMAT_VERSION);
        }
        if (loaded.getWindowSizeMs() != props.getWindowSizeMs()
                || loaded.getAllowedLatenessMs() != props.getAllowedLatenessMs()) {
            throw new CheckpointCorruptedException(
                    "checkpoint parameters do not match current configuration: checkpoint windowSizeMs="
                            + loaded.getWindowSizeMs() + " allowedLatenessMs=" + loaded.getAllowedLatenessMs()
                            + " vs config windowSizeMs=" + props.getWindowSizeMs()
                            + " allowedLatenessMs=" + props.getAllowedLatenessMs());
        }
        long lastOffset = loaded.getLastConsumedOffset();
        if (lastOffset >= walRecords) {
            throw new CheckpointCorruptedException(
                    "checkpoint lastConsumedOffset=" + lastOffset + " points beyond WAL which holds "
                            + walRecords + " record(s)");
        }

        if (lastOffset >= 0) {
            final String[] rawAtOffset = new String[1];
            try {
                wal.replay(lastOffset, rec -> {
                    if (rec.offset() == lastOffset) {
                        rawAtOffset[0] = rec.rawJson();
                    }
                });
            } catch (Exception e) {
                throw new CheckpointCorruptedException("cannot cross-check checkpoint against WAL", e);
            }
            if (rawAtOffset[0] == null) {
                throw new CheckpointCorruptedException(
                        "checkpoint references offset " + lastOffset + " which is absent from the WAL");
            }
            String actualDigest = CheckpointStore.sha256(rawAtOffset[0].getBytes(StandardCharsets.UTF_8));
            if (loaded.getLastRecordDigest() == null
                    || !actualDigest.equals(loaded.getLastRecordDigest())) {
                throw new CheckpointCorruptedException(
                        "checkpoint/WAL mismatch at offset " + lastOffset
                                + ": stored digest does not match WAL content (state and position diverged)");
            }
        }
        log.info("checkpoint validated: resuming after offset={} ({} WAL record(s) present)",
                lastOffset, walRecords);
        return loaded;
    }

    private void replayUnconsumed() {
        long fromOffset = state.getLastConsumedOffset() + 1;
        long walRecords = wal.size();
        if (fromOffset >= walRecords) {
            return;
        }
        long remaining = walRecords - fromOffset;
        log.info("replaying {} unconsumed WAL record(s) from offset {}", remaining, fromOffset);
        try {
            wal.replay(fromOffset, record -> aggregator.process(record.offset(), record.rawJson()));
        } catch (Exception e) {
            throw new CheckpointCorruptedException("WAL replay from offset " + fromOffset + " failed", e);
        }
    }

    /**
     * Durably appends the event to the WAL first, then hands it to the
     * bounded queue. Returns null on success or a stable rejection code.
     */
    public IngestResult ingest(String rawJson) {
        long offset = wal.appendRaw(rawJson);
        metrics.ingested.incrementAndGet();
        if (!queue.tryEnqueue(new PendingEvent(offset, rawJson))) {
            metrics.rejectedQueueFull.incrementAndGet();
            return new IngestResult(false, 503, "QUEUE_FULL",
                    offset, "event is durable in the WAL but the bounded processing queue is full; "
                            + "retry later or wait for the consumer to drain the backlog");
        }
        return new IngestResult(true, 202, "ACCEPTED", offset, "event persisted to WAL and queued");
    }

    public record IngestResult(boolean accepted, int httpStatus, String code, long offset, String message) {
    }

    private void consumeLoop() {
        while (running.get()) {
            try {
                PendingEvent event = queue.poll(props.getPollIdleMs(), TimeUnit.MILLISECONDS);
                if (event != null) {
                    aggregator.process(event.offset(), event.rawJson());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                log.error("consumer loop error", e);
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (consumerThread != null) {
            // Do not interrupt mid-checkpoint: let an in-flight event finish
            // its atomic commit, otherwise the consumer could be killed
            // between state mutation and checkpoint persistence.
            long deadline = System.currentTimeMillis() + 2_000;
            while (queueSize() > 0 && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(5);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            Thread.yield();
            try {
                consumerThread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (consumerThread.isAlive()) {
                consumerThread.interrupt();
            }
        }
        if (wal != null) {
            wal.close();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MIN_VALUE + 100;
    }

    public Aggregator aggregator() {
        return aggregator;
    }

    public DeadLetterStore deadLetters() {
        return deadLetterStore;
    }

    public ServiceMetrics metrics() {
        return metrics;
    }

    public FileResultSink fileSink() {
        return fileSink;
    }

    public long queueSize() {
        return queue == null ? -1 : queue.size();
    }

    public int queueCapacity() {
        return props.getQueueCapacity();
    }

    public long lastConsumedOffset() {
        return state == null ? -1 : state.getLastConsumedOffset();
    }
}
