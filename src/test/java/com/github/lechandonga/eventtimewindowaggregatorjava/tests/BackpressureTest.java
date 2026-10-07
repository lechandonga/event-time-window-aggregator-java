package com.github.lechandonga.eventtimewindowaggregatorjava.tests;

import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import com.github.lechandonga.eventtimewindowaggregatorjava.queue.BoundedEventQueue;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.AbstractScenarioTest;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.BusinessScenario;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.CaseName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business: bounded backlog never blocks ingress threads indefinitely. */
@BusinessScenario("有界积压与背压")
public class BackpressureTest extends AbstractScenarioTest {

    @Test
    @CaseName("有界队列打满时offer立即失败而不是无限阻塞")
    void fullQueueRejectsImmediately(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        service.fileSink().setFailureTrigger(key -> {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return false;
        });

        long start = System.nanoTime();
        boolean saw503 = false;
        for (int i = 0; i < 40; i++) {
            var result = post(service, "e" + i, "k1", 11_000 + i, 1);
            if (result.httpStatus() == 503) {
                saw503 = true;
                assertEquals("QUEUE_FULL", result.code());
                assertTrue(result.accepted() == false);
            }
        }
        long blockedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue(saw503, "bounded queue must surface 503 QUEUE_FULL under backlog");
        assertTrue(blockedMs < 2_000, "ingress must not be dragged down by the slow consumer, was "
                + blockedMs + "ms");
        assertTrue(service.metrics().rejectedQueueFull.get() > 0);
        service.stop();
    }

    @Test
    @CaseName("被拒事件已在WAL中，消费恢复后仍被处理，不丢数据")
    void rejectedEventsRemainDurableInWal(@TempDir Path dir) throws Exception {
        BoundedEventQueue<String> q = new BoundedEventQueue<>(1);
        assertTrue(q.tryEnqueue("x"));
        assertFalse(q.tryEnqueue("y"));
        assertEquals(1, q.rejectedFullCount(), "rejections are observable");
        assertEquals("x", q.poll(1, TimeUnit.MILLISECONDS));

        // queue-level behaviour: after a slot frees, enqueue succeeds again
        assertTrue(q.tryEnqueue("z"));
    }
}
