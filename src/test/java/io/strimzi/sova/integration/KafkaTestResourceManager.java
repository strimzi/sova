/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.integration;

import java.util.Map;

import io.strimzi.test.container.StrimziKafkaCluster;

/**
 * Manages a Strimzi embedded Kafka cluster for integration tests.
 */
public class KafkaTestResourceManager {
    // Per instance, not static: two integration tests each manage their own cluster, and a shared
    // static field would have one test's stop() tear down the other's broker as soon as the two
    // ran concurrently. Delegates straight to the cluster instead of caching a second, redundant
    // copy of the address in its own field.
    private volatile StrimziKafkaCluster kafka;

    /**
     * Returns the bootstrap server address reachable from the host (after {@link #start()}).
     *
     * @return Kafka bootstrap server address
     */
    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    /**
     * Starts the embedded Kafka cluster on the shared Docker network.
     */
    public void start() {
        kafka = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withSharedNetwork()
                .withAdditionalKafkaConfiguration(Map.of(
                        "auto.create.topics.enable", "true"))
                .build();
        kafka.start();
    }

    /**
     * Stops the embedded Kafka cluster.
     */
    public void stop() {
        if (kafka != null) {
            kafka.stop();
        }
    }
}
