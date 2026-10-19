/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers shared by the three collectors.
 *
 * <p>Threading: stateless utility class — thread-safe.
 */
final class CollectorUtils {

    private static final Logger log = LoggerFactory.getLogger(CollectorUtils.class);

    private CollectorUtils() {
    }

    /**
     * Applies the include/exclude pair to one topic or consumer group name.
     *
     * @param value   the name to test
     * @param include compiled include pattern; must match the whole value
     * @param exclude compiled exclude pattern, absent when nothing is excluded
     * @return {@code true} when the value should be reported
     */
    static boolean matches(String value, Pattern include, Optional<Pattern> exclude) {
        if (!include.matcher(value).matches()) {
            return false;
        }

        return exclude.isEmpty() || !exclude.get()
            .matcher(value)
            .matches();
    }

    /**
     * Resolves a map of per-element AdminClient futures, dropping the ones that failed.
     *
     * <p>The batch variants ({@code allTopicNames()}, {@code all()}) fail wholesale if any single
     * element fails, so a topic or group deleted between the list call and the describe call costs
     * the entire collection cycle. On a busy cluster that is routine. Resolving element by element
     * degrades instead: the deleted entry is dropped from this cycle and everything else is
     * published.
     *
     * <p>Dropping elements is only safe while <em>some</em> of them resolve. If every element of a
     * non-empty batch fails — an ACL change that revokes Describe, say — the caller would receive
     * an empty result indistinguishable from a cluster that genuinely has no topics or groups, and
     * would publish it as a successful cycle: the staleness timestamp advances, readiness stays UP,
     * and every affected series silently vanishes from the scrape. Threshold alerts then stop
     * firing rather than firing. So a wholly failed batch throws instead.
     *
     * <p>On a one-element batch this cannot be told apart from a single routine deletion, so such
     * a cycle is skipped where it used to degrade. That is harmless: readiness only trips after two
     * stale intervals, so one skipped cycle never flips the probe, while a persistent total failure
     * correctly does.
     *
     * @param futures per-element futures keyed by topic name, group id, or partition
     * @param kind    what the keys are, used in the log and failure messages
     * @param <K>     key type
     * @param <V>     resolved value type
     * @return the successfully resolved entries, never null
     * @throws InterruptedException    if the collecting thread is interrupted
     * @throws CollectionFailedException if every element of a non-empty batch failed
     */
    static <K, V> Map<K, V> resolveAll(Map<K, KafkaFuture<V>> futures, String kind)
        throws InterruptedException, CollectionFailedException {
        Map<K, V> resolved = new HashMap<>(futures.size());
        Throwable lastFailure = null;
        for (Map.Entry<K, KafkaFuture<V>> entry : futures.entrySet()) {
            try {
                resolved.put(entry.getKey(), entry.getValue().get());
            } catch (ExecutionException e) {
                lastFailure = e.getCause() != null ? e.getCause() : e;
                log.debug("Skipping {} '{}' this cycle: {}", kind, entry.getKey(), lastFailure.toString());
            }
        }

        int failed = futures.size() - resolved.size();
        if (failed == 0) {
            return resolved;
        }

        if (resolved.isEmpty()) {
            throw new CollectionFailedException("All " + futures.size() + " " + kind + " lookups failed this cycle", lastFailure);
        }

        // Partial failure is the case this helper exists for, but it must not be invisible: a
        // steadily rising count is how a half-broken cluster or a partial ACL change shows up.
        log.warn("Dropped {} of {} {} results this cycle, last failure: {}",
            failed, futures.size(), kind, lastFailure.toString());
        return resolved;
    }
}
