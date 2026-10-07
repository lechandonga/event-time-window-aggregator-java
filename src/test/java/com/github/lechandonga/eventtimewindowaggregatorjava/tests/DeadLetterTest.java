package com.github.lechandonga.eventtimewindowaggregatorjava.tests;

import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetter;
import com.github.lechandonga.eventtimewindowaggregatorjava.model.DeadLetterReason;
import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.AbstractScenarioTest;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.BusinessScenario;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.CaseName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business: late, malformed and sink-failed events get distinguishable DLQ reasons. */
@BusinessScenario("死信路径：迟到/坏消息/写出失败")
public class DeadLetterTest extends AbstractScenarioTest {

    @Test
    @CaseName("超出容忍范围的迟到事件进入TOO_LATE死信且不改动已关闭窗口")
    void tooLateEventsAreDeadLetteredWithoutMutation(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        post(service, "a", "k1", 11_000, 4);
        post(service, "close", "k1", 30_000, 0);
        awaitDrained(service, 2);

        long sumBefore = service.aggregator().visibleResults(10_000L, "k1").getFirst().sum();

        // wm is now 25_000, window [10000,20000) is closed: an event at
        // 10_500 is far beyond allowed lateness and must be rejected into DLQ.
        post(service, "late-1", "k1", 10_500, 99);
        awaitDrained(service, 3);

        long sumAfter = service.aggregator().visibleResults(10_000L, "k1").getFirst().sum();
        assertEquals(sumBefore, sumAfter, "closed window result is immutable");

        DeadLetter dl = service.deadLetters().all().stream()
                .filter(d -> DeadLetterReason.TOO_LATE.code().equals(d.reason()))
                .findFirst().orElseThrow(() -> new AssertionError("expected a TOO_LATE dead letter"));
        assertEquals("late-1", dl.eventId());
        assertTrue(dl.detail().contains("already closed"));
        assertEquals(1, service.metrics().late.get());

        String dlqFile = Files.readString(service.deadLetters().path());
        assertTrue(dlqFile.contains("TOO_LATE"), "dead letter must be persisted locally");
        service.stop();
    }

    @Test
    @CaseName("格式损坏的WAL记录进入MALFORMED死信且消费继续推进")
    void malformedEventsAreDeadLetteredAndConsumptionContinues(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        service.ingest("{not-json");
        post(service, "good", "k1", 11_000, 8);
        post(service, "close", "k1", 30_000, 0);
        awaitDrained(service, 3);

        DeadLetter dl = service.deadLetters().all().stream()
                .filter(d -> DeadLetterReason.MALFORMED.code().equals(d.reason()))
                .findFirst().orElseThrow(() -> new AssertionError("expected MALFORMED dead letter"));
        assertTrue(dl.detail().contains("raw="));
        assertEquals(1, service.metrics().malformed.get());
        assertEquals(8, service.aggregator().visibleResults(10_000L, "k1").getFirst().sum());
        service.stop();
    }

    @Test
    @CaseName("Sink重试耗尽后结果进入SINK_RETRY_EXHAUSTED死信并带原因")
    void sinkRetryExhaustionIsDeadLettered(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        service.fileSink().setFailureTrigger("poison"::equals);

        post(service, "p1", "poison", 11_000, 1);
        post(service, "close", "poison", 30_000, 0);
        awaitDrained(service, 2);

        DeadLetter dl = service.deadLetters().all().stream()
                .filter(d -> DeadLetterReason.SINK_RETRY_EXHAUSTED.code().equals(d.reason()))
                .findFirst().orElseThrow(() -> new AssertionError("expected SINK_RETRY_EXHAUSTED dead letter"));
        assertTrue(dl.detail().contains("attempts"));
        assertEquals(3, service.metrics().sinkRetried.get() + 1,
                "first try + 2 retries = 3 attempts");
        assertFalse(service.aggregator().visibleResults(10_000L, "poison").isEmpty(),
                "aggregation state is still consistent and queryable even if the sink failed");

        // a healthy key in a later window still sinks normally
        service.fileSink().setFailureTrigger(null);
        post(service, "h1", "healthy", 31_000, 5);
        post(service, "hclose", "healthy", 50_000, 0);
        awaitDrained(service, 4);
        assertFalse(service.aggregator().visibleResults(30_000L, "healthy").isEmpty());
        service.stop();
    }
}
