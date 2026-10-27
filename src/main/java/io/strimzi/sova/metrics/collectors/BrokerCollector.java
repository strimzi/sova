/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects broker-level metadata via {@code AdminClient.describeCluster()}.
 * <p>
 * Broker count and addresses are exposed as {@code kafka_brokers} and {@code kafka_broker_info} metrics.
 */
final class BrokerCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCollector.class);

    private BrokerCollector() { }

    /**
     * Holds broker count and address map.
     *
     * @param brokerCount number of brokers
     * @param addresses   broker address map
     */
    record BrokerData(int brokerCount, Map<String, String> addresses) { }

    /**
     * Describes the cluster and collects broker metadata.
     *
     * @param admin Kafka AdminClient
     * @return broker data from {@code AdminClient.describeCluster()}
     * @throws ExecutionException   on AdminClient error
     * @throws InterruptedException on thread interrupt
     */
    static BrokerData collect(Admin admin) throws ExecutionException, InterruptedException {
        Collection<Node> nodes = admin.describeCluster().nodes().get();
        Map<String, String> addresses = new HashMap<>();
        for (Node node : nodes) {
            addresses.put(String.valueOf(node.id()), node.host() + ":" + node.port());
        }
        LOGGER.debug("Discovered {} broker(s)", nodes.size());
        return new BrokerData(nodes.size(), addresses);
    }
}
