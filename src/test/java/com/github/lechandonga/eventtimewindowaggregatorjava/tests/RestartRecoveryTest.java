package com.github.lechandonga.eventtimewindowaggregatorjava.tests;

import com.github.lechandonga.eventtimewindowaggregatorjava.model.WindowResult;
import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.AbstractScenarioTest;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.BusinessScenario;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.CaseName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business: crash/restart with offset + state checkpointed atomically. */
@BusinessScenario("崩溃重启恢复（位点与状态原子检查点）")
public class RestartRecoveryTest extends AbstractScenarioTest {

    @Test
    @CaseName("已落WAL未消费的事件在重启后继续消费，结果不漏不重")
    void unconsumedWalReplayedAfterRestart(@TempDir Path dir) throws Exception {
        ProcessingService first = startService(dir);
        post(first, "a", "k1", 11_000, 2);
        post(first, "b", "k1", 12_000, 3);
        post(first, "c", "k1", 13_000, 5);
        post(first, "d", "k1", 30_000, 0);
        awaitDrained(first, 4);

        // Simulate a second life-cycle: events that were durable in the WAL
        // before a hard kill, with the process starting again before the
        // consumer thread drains them (queue capacity 1 forces backpressure
        // while the WAL keeps every event).
        first.stop();

        ProcessingService second = startService(dir);
        List<WindowResult> results = second.aggregator().visibleResults(10_000L, "k1");
        assertEquals(1, results.size());
        WindowResult window = results.getFirst();
        assertEquals(3, window.count(), "a+b+c all recovered from the WAL exactly once");
        assertEquals(10, window.sum());
        assertEquals(3, second.lastConsumedOffset());
        second.stop();
    }

    @Test
    @CaseName("多次重启幂等：最终窗口结果与一次连续运行一致")
    void multipleRestartsAreIdempotent(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        post(service, "a", "k1", 11_000, 1);
        awaitDrained(service, 1);
        service.stop();

        for (int i = 0; i < 3; i++) {
            service = startService(dir);
            assertTrue(service.aggregator().visibleResults(10_000L, "k1").isEmpty());
            service.stop();
        }

        service = startService(dir);
        post(service, "b", "k1", 12_000, 9);
        post(service, "close", "k1", 30_000, 0);
        awaitDrained(service, 3);
        WindowResult window = service.aggregator().visibleResults(10_000L, "k1").getFirst();
        assertEquals(2, window.count());
        assertEquals(10, window.sum());
        service.stop();
    }
}
