/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

/**
 * Thrown when a collection branch gathered nothing it could publish.
 *
 * <p>Raised by {@link CollectorUtils#resolveAll} when every element of a non-empty batch of
 * per-element futures failed. The distinction that matters is between "the cluster has no topics"
 * and "we were not allowed to ask about any of them": both produce an empty map, but only the
 * first is a successful cycle.
 *
 * <p>Checked, and deliberately not caught inside the collectors. It propagates out of the branch
 * through {@code ExecutionException} to {@link MetricsCollector#collect()}, which logs it at WARN
 * and returns without updating the registry. The previous snapshot keeps being served and ages;
 * if the failure persists, readiness goes DOWN on its own after two collection intervals, which is
 * the only readiness mechanism this codebase has (see invariant 9 in {@code AGENTS.md}).
 */
public class CollectionFailedException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs the exception.
     *
     * @param message what failed, naming the kind of lookup and how many were attempted
     * @param cause   the last underlying failure seen in the batch, or {@code null} if none was
     *                captured
     */
    public CollectionFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
