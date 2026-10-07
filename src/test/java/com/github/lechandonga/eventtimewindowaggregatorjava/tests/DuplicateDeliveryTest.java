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
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business: at-least-once redelivery must never double-count. */
@BusinessScenario("重复投递幂等去重")
public class DuplicateDeliveryTest extends AbstractScenarioTest {

    @Test
    @CaseName("同一事件ID被HTTP重复提交多次，窗口内只计数一次")
    void duplicateHttpPostsCountedOnce(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);

        for (int i = 0; i < 5; i++) {
            var result = post(service, "dup-1", "k1", 12_000, 7);
            assertTrue(result.accepted());
        }
        post(service, "other", "k1", 12_100, 3);
        post(service, "closer", "k1", 30_000, 0);
        awaitDrained(service, 7);

        WindowResult window = service.aggregator().visibleResults(10_000L, "k1").getFirst();
        assertEquals(2, window.count(), "dup-1 counted once plus other");
        assertEquals(10, window.sum());
        assertEquals(4, service.metrics().duplicate.get(),
                "each redelivery is observable as duplicate in metrics");
        service.stop();
    }

    @Test
    @CaseName("进程重启后重放已消费记录不会造成重复计数")
    void duplicateAfterRestartCountedOnce(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        post(service, "a", "k1", 12_000, 4);
        post(service, "b", "k1", 13_000, 6);
        post(service, "c", "k1", 30_000, 0);
        awaitDrained(service, 3);

        WindowResult before = service.aggregator().visibleResults(10_000L, "k1").getFirst();
        assertEquals(2, before.count());
        service.stop();

        ProcessingService restarted = startService(dir);
        WindowResult after = restarted.aggregator().visibleResults(10_000L, "k1").getFirst();
        assertEquals(before, after, "restart replay must not change or duplicate results");
        assertEquals(2, after.count());
        assertEquals(10, after.sum());
        restarted.stop();
    }
}
