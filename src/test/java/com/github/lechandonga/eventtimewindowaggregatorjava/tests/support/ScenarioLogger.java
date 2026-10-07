package com.github.lechandonga.eventtimewindowaggregatorjava.tests.support;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Prints a readable coverage ledger while tests run:
 * business scenario header, per-case PASS/FAIL/SKIP lines and a per-business
 * summary, so test logs show which businesses are covered and each case's result.
 */
public class ScenarioLogger implements BeforeAllCallback, BeforeEachCallback, TestWatcher, AfterAllCallback {

    private static final Logger log = LoggerFactory.getLogger("SCENARIO-LEDGER");

    private int passed;
    private int failed;
    private int skipped;
    private String business;

    @Override
    public void beforeAll(ExtensionContext context) {
        business = Optional.ofNullable(context.getRequiredTestClass().getAnnotation(BusinessScenario.class))
                .map(BusinessScenario::value)
                .orElse(context.getRequiredTestClass().getSimpleName());
        log.info("");
        log.info("================ 业务场景覆盖: {} ================", business);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        log.info("---- 执行用例: {} | {}", context.getDisplayName(), caseName(context));
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        passed++;
        log.info("[PASS] 用例: {} | {}", context.getDisplayName(), caseName(context));
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        failed++;
        log.error("[FAIL] 用例: {} | {} -> {}", context.getDisplayName(), caseName(context),
                cause == null ? "" : cause.toString());
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        skipped++;
        log.warn("[SKIP] 用例: {} | {}", context.getDisplayName(), caseName(context));
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        skipped++;
        log.warn("[SKIP] 用例: {} | {} ({})", context.getDisplayName(), caseName(context),
                reason.orElse("disabled"));
    }

    @Override
    public void afterAll(ExtensionContext context) {
        int total = passed + failed + skipped;
        log.info("---------------- 业务[{}]用例汇总: 共 {} 个, PASS={}, FAIL={}, SKIP={} ----------------",
                business, total, passed, failed, skipped);
        log.info("");
    }

    private String caseName(ExtensionContext context) {
        return context.getTestMethod()
                .map(m -> m.isAnnotationPresent(CaseName.class) ? m.getAnnotation(CaseName.class).value() : "-")
                .orElse("-");
    }
}
