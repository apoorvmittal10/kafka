/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryExporterProvider;
import org.apache.kafka.test.TestUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.clients.admin.AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that each client type makes its final, terminating metrics push (KIP-714) on close.
 * The subscription push interval is one hour so the only push the broker can see is the terminating one.
 */
public class ClientTelemetryTerminatingPushTest {

    private static final String TOPIC = "terminating_push_topic";
    private static final Set<Uuid> TERMINATING_PUSHES = ConcurrentHashMap.newKeySet();

    @ClusterTest(
        types = Type.KRAFT,
        serverProperties = {
            @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = "org.apache.kafka.clients.admin.ClientTelemetryTerminatingPushTest$TelemetryExporter"),
        })
    public void testTerminatingPush(ClusterInstance cluster) throws Exception {
        setUpSubscriptionAndTopic(cluster);
        List<String> missing = new ArrayList<>();

        Uuid producerId;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>(TOPIC, 0, null, "bar")).get();
            producerId = producer.clientInstanceId(Duration.ofSeconds(10));
            assertNotNull(producerId);
            assertFalse(TERMINATING_PUSHES.contains(producerId));
        }
        checkTerminatingPush(missing, "producer", producerId);

        for (GroupProtocol protocol : GroupProtocol.values()) {
            Uuid consumerId;
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol.name(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
                consumer.assign(List.of(new TopicPartition(TOPIC, 0)));
                consumer.poll(Duration.ofSeconds(1));
                consumerId = consumer.clientInstanceId(Duration.ofSeconds(10));
                assertNotNull(consumerId);
                assertFalse(TERMINATING_PUSHES.contains(consumerId));
            }
            checkTerminatingPush(missing, "consumer (" + protocol + ")", consumerId);
        }

        Uuid adminId;
        try (Admin admin = Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, true))) {
            adminId = admin.clientInstanceId(Duration.ofSeconds(10));
            assertNotNull(adminId);
            assertFalse(TERMINATING_PUSHES.contains(adminId));
        }
        checkTerminatingPush(missing, "admin", adminId);

        assertTrue(missing.isEmpty(), "No terminating push received on close for: " + missing);
    }

    private static void checkTerminatingPush(List<String> missing, String client, Uuid id) throws Exception {
        try {
            TestUtils.waitForCondition(() -> TERMINATING_PUSHES.contains(id), 10_000, "no terminating push");
        } catch (AssertionError e) {
            missing.add(client);
        }
    }

    private static void setUpSubscriptionAndTopic(ClusterInstance cluster) throws Exception {
        cluster.createTopic(TOPIC, 1, (short) 1);
        try (Admin admin = cluster.admin()) {
            ConfigResource subscription = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "terminating-push");
            admin.incrementalAlterConfigs(Map.of(subscription, List.of(
                new AlterConfigOp(new ConfigEntry("metrics", "*"), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry("interval.ms", "3600000"), AlterConfigOp.OpType.SET)))).all().get();
        }
    }

    @SuppressWarnings("unused")
    public static class TelemetryExporter implements ClientTelemetryExporterProvider, MetricsReporter {

        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ClientTelemetryExporter clientTelemetryExporter() {
            return (context, payload) -> {
                if (payload.isTerminating()) {
                    TERMINATING_PUSHES.add(payload.clientInstanceId());
                }
            };
        }
    }
}
