package com.github.lechandonga.eventtimewindowaggregatorjava.tests;

import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;
import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.AbstractScenarioTest;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.BusinessScenario;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.CaseName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business: event-time tumbling windows under out-of-order arrival. */
@BusinessScenario("乱序事件的事件时间窗口聚合")
public class OutOfOrderWindowTest extends AbstractScenarioTest {

    private WindowResult result(ProcessingService service, long windowStart, String key) {
        return service.aggregator().visibleResults(windowStart, key).stream()
                .findFirst().orElseThrow(() -> new AssertionError("no visible result for " + windowStart + "/" + key));
    }

    @Test
    @CaseName("逆序到达的事件与顺序到达产生完全一致的窗口结果")
    void outOfOrderProducesSameResultAsInOrder(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);

        long[] times = {19_999, 10_001, 15_000, 26_000};
        long valueSum = 0;
        for (int i = 0; i < times.length; i++) {
            assertTrue(post(service, "e" + i, "k1", times[i], i + 1).accepted());
            if (times[i] < 20_000) {
                valueSum += i + 1;
            }
        }
        awaitDrained(service, 4);
        service.stop();

        ProcessingService expected = startService(dir.resolveSibling(dir.getFileName() + "-ref"));
        long[] sorted = {10_001, 15_000, 19_999, 26_000};
        for (int i = 0; i < sorted.length; i++) {
            assertTrue(post(expected, "ref-e" + i, "k1", sorted[i], i + 1).accepted());
        }
        awaitDrained(expected, 4);

        ProcessingService restarted = startService(dir);
        WindowResult outOfOrder = result(restarted, 10_000, "k1");
        WindowResult inOrder = result(expected, 10_000, "k1");
        assertEquals(inOrder.count(), outOfOrder.count());
        assertEquals(inOrder.sum(), outOfOrder.sum());
        assertEquals(inOrder.firstEventTime(), outOfOrder.firstEventTime());
        assertEquals(inOrder.lastEventTime(), outOfOrder.lastEventTime());
        assertEquals(3, inOrder.count(), "three events in [10000,20000), 26000 only closes it");
        assertEquals(valueSum, inOrder.sum());
        assertEquals(1 + 2 + 3, valueSum);
        restarted.stop();
        expected.stop();
    }

    @Test
    @CaseName("窗口关闭后、容忍范围内的迟到事件仍计入且结果不变顺序")
    void allowedLatenessLateButInRangeEventIsCounted(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);

        // a@19_999 -> wm 14_999: window [10000,20000) open.
        assertTrue(post(service, "a", "k1", 19_999, 10).accepted());
        awaitDrained(service, 1);
        assertTrue(service.aggregator().visibleResults().isEmpty());

        // b@19_500 arrives out of order while the window is still alive
        // (wm 14_999 < windowEnd 20_000) and must be counted into it.
        assertTrue(post(service, "b", "k1", 19_500, 20).accepted());
        awaitDrained(service, 2);
        assertTrue(service.aggregator().visibleResults().isEmpty());

        // c@26_000 -> wm 21_000 closes window [10000,20000) and publishes it.
        assertTrue(post(service, "c", "k1", 26_000, 1).accepted());
        awaitDrained(service, 3);

        WindowResult closed = result(service, 10_000, "k1");
        assertEquals(2, closed.count());
        assertEquals(10 + 20, closed.sum());
        service.stop();
    }

    @Test
    @CaseName("未关闭窗口的中间结果对外不可见")
    void openWindowNeverVisible(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        assertTrue(post(service, "a", "k1", 1_000, 5).accepted());
        assertTrue(post(service, "b", "k1", 2_000, 7).accepted());
        awaitDrained(service, 2);

        assertTrue(service.aggregator().visibleResults().isEmpty(),
                "no window may be published before it closes");
        assertEquals(0, service.metrics().windowsClosed.get());
        service.stop();

        // restarting must not retroactively publish the still-open window either
        ProcessingService restarted = startService(dir);
        assertFalse(restarted.aggregator().visibleResults().stream().findAny().isPresent());
        restarted.stop();
    }
}
