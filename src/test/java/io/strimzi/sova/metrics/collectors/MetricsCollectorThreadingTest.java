/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricsRegistry;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for how {@link MetricsCollector} treats the threads it runs on and the threads it
 * starts. No Kafka broker is involved.
 */
class MetricsCollectorThreadingTest {
    private static final SovaConfig CONFIG = SovaConfig.forTesting("localhost:9092");

    /**
     * An {@link Error} thrown on the scheduler thread must not escape {@code collect()}: a
     * scheduled task that throws is cancelled and never runs again.
     */
    @Test
    void anErrorInOneCycleDoesNotStopTheSchedule() throws InterruptedException {
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch secondCycleRan = new CountDownLatch(1);
        MetricsCollector collector = new MetricsCollector(null, new MetricsRegistry(), CONFIG) {
            @Override
            void collectInternal() {
                if (runs.incrementAndGet() == 1) {
                    throw new NoClassDefFoundError("simulated");
                }
                secondCycleRan.countDown();
            }
        };

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        try {
            scheduler.scheduleWithFixedDelay(collector::collect, 0, 10, TimeUnit.MILLISECONDS);

            assertTrue(secondCycleRan.await(5, TimeUnit.SECONDS),
                    "the cycle after an Error must still run, or collection stops for good");
        } finally {
            scheduler.shutdownNow();
        }
    }

    /**
     * A cycle interrupted by shutdown must leave the interrupt flag set: {@code collect()}
     * swallows the {@link InterruptedException} so the schedule survives, and the executor
     * shutting the pod down cannot finish tearing down if the flag is lost with it.
     */
    @Test
    void anInterruptedCycleRestoresTheInterruptFlag() {
        MetricsCollector collector = new MetricsCollector(null, new MetricsRegistry(), CONFIG) {
            @Override
            void collectInternal() throws InterruptedException {
                throw new InterruptedException("simulated shutdown");
            }
        };

        collector.collect();

        // Thread.interrupted() reads and clears, so the rest of the suite runs uninterrupted.
        assertTrue(Thread.interrupted(),
                "the interrupt flag must be restored after collect() swallows the interruption");
    }

    /**
     * One branch fails at once while another is still blocked in the AdminClient and is slow to
     * unwind. When {@code collect()} returns, that branch thread must already be finished.
     *
     * <p>The failing branch waits on a latch the slow branch counts down right after entering its
     * blocking call, so the slow branch is guaranteed to already be inside {@code Thread.sleep}
     * before the failure (and the resulting {@code shutdownNow()}) happens. Without this
     * synchronization, a task cancelled by {@code shutdownNow()} before it is scheduled to run
     * never sets {@code slowBranchFinished} at all, and the assertion passes for the wrong reason
     * almost every time, only occasionally actually exercising the 60-second sleep it means to
     * guard.
     */
    @Test
    void noBranchThreadOutlivesAFailedCycle() {
        AtomicBoolean slowBranchFinished = new AtomicBoolean();
        CountDownLatch slowBranchEntered = new CountDownLatch(1);
        Admin admin = (Admin) Proxy.newProxyInstance(
                Admin.class.getClassLoader(), new Class<?>[] {Admin.class},
                (proxy, method, args) -> {
                    if ("listTopics".equals(method.getName())) {
                        slowBranchEntered.countDown();
                        try {
                            Thread.sleep(60_000);
                        } catch (InterruptedException e) {
                            // Interrupted as the cycle is abandoned; take a while to unwind.
                            Thread.sleep(300);
                        }
                        slowBranchFinished.set(true);
                    } else {
                        // Do not fail until the slow branch is demonstrably blocked in its sleep,
                        // otherwise collect() can abandon the cycle before that branch even starts.
                        // Any assertion failure here would only be swallowed by collect()'s own
                        // catch-and-log, so this is a best-effort wait, not a second assertion.
                        slowBranchEntered.await(5, TimeUnit.SECONDS);
                    }
                    throw new IllegalStateException("simulated failure in " + method.getName());
                });

        new MetricsCollector(admin, new MetricsRegistry(), CONFIG).collect();

        assertTrue(slowBranchFinished.get(),
                "collect() must wait for its branch threads, not just interrupt them");
    }
}
