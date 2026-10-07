package com.github.lechandonga.eventtimewindowaggregatorjava.tests;

import com.github.lechandonga.eventtimewindowaggregatorjava.proc.ProcessingService;
import com.github.lechandonga.eventtimewindowaggregatorjava.state.CheckpointCorruptedException;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.AbstractScenarioTest;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.BusinessScenario;
import com.github.lechandonga.eventtimewindowaggregatorjava.tests.support.CaseName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Business: recovery refuses unknown intermediate states. */
@BusinessScenario("检查点完整性校验（缺失/损坏/不一致拒绝恢复）")
public class CheckpointIntegrityTest extends AbstractScenarioTest {

    @Test
    @CaseName("WAL存在但检查点缺失时拒绝启动并给出明确报错")
    void missingCheckpointWithWalIsRejected(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        post(service, "a", "k1", 11_000, 1);
        awaitDrained(service, 1);
        service.stop();
        Files.delete(dir.resolve("checkpoint.json"));

        CheckpointCorruptedException error = assertThrows(CheckpointCorruptedException.class,
                () -> startService(dir));
        assertTrue(error.getMessage().contains("checkpoint"));
        assertTrue(error.getMessage().contains("missing"));
    }

    @Test
    @CaseName("检查点被篡改/截断时校验和不匹配并拒绝恢复")
    void corruptCheckpointIsRejected(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        post(service, "a", "k1", 11_000, 1);
        awaitDrained(service, 1);
        service.stop();

        Path cp = dir.resolve("checkpoint.json");
        String tampered = Files.readString(cp, StandardCharsets.UTF_8).replace("\"count\":1",
                "\"count\":999");
        Files.writeString(cp, tampered, StandardCharsets.UTF_8);

        CheckpointCorruptedException error = assertThrows(CheckpointCorruptedException.class,
                () -> startService(dir));
        assertTrue(error.getMessage().contains("checksum mismatch"), error.getMessage());
    }

    @Test
    @CaseName("检查点窗口参数与当前配置不一致时拒绝恢复")
    void configMismatchIsRejected(@TempDir Path dir) throws Exception {
        ProcessingService service = startService(dir);
        post(service, "a", "k1", 11_000, 1);
        awaitDrained(service, 1);
        service.stop();

        var differentProps = props(dir);
        differentProps.setWindowSizeMs(60_000);
        ProcessingService incompatible = new ProcessingService(differentProps, MAPPER);
        CheckpointCorruptedException error = assertThrows(CheckpointCorruptedException.class,
                incompatible::start);
        assertTrue(error.getMessage().contains("do not match"));
    }
}
