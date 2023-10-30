package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;
import javax.management.MalformedObjectNameException;
import org.apache.kafka.common.metrics.MetricsContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClientTelemetryReporterTest {

    private ClientTelemetryReporter reporter;
    private Map<String, Object> configs;

    private MetricsContext ctx = null;
    private MetricsContext ctxWithCluster = null;

    @BeforeEach
    public void setUp() {
        reporter = new ClientTelemetryReporter();
        configs = new HashMap<>();
//        configs.put(KafkaConfig.BrokerIdProp(), "1");
//        configs.put(KafkaConfig.LogDirsProp(), System.getProperty("java.io.tmpdir"));
//
//        ProviderRegistry.registerProvider(MockProvider.NAMESPACE, MockProvider.class.getCanonicalName());
//        Map<String, Object> metadata = new HashMap<>();
//        metadata.put(ConfluentConfigs.RESOURCE_LABEL_TYPE, "MOCK");
//        metadata.put(Utils.RESOURCE_LABEL_CLUSTER_ID, "foo");
//        metadata.put(ConfluentConfigs.RESOURCE_LABEL_VERSION, "v1");
//
//        metadata.putAll(ImmutableMap.of(
//                ConfluentConfigs.RESOURCE_LABEL_TYPE, "MOCK",
//                Utils.RESOURCE_LABEL_CLUSTER_ID, "foo",
//                ConfluentConfigs.RESOURCE_LABEL_VERSION, "v1"
//        ));
//        ctx = new KafkaMetricsContext(MockProvider.NAMESPACE, metadata);
//
//        Map<String, Object> metadataWtihCluster = new HashMap<>();
//        metadataWtihCluster.putAll(ImmutableMap.of(
//                ConfluentConfigs.RESOURCE_LABEL_TYPE, "MOCK",
//                Utils.RESOURCE_LABEL_CLUSTER_ID, "foo",
//                ConfluentConfigs.RESOURCE_LABEL_VERSION, "v1",
//                Utils.KAFKA_CLUSTER_ID, "clusterid",
//                Utils.KAFKA_BROKER_ID, "brokerid"
//        ));
//        ctxWithCluster = new KafkaMetricsContext(MockProvider.NAMESPACE, metadataWtihCluster);

    }

    @AfterEach
    public void tearDown() {
        reporter.close();
    }

    @Test
    public void testOnUpdateInvalidRegex() {
//        configs.put(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, "(.");
//        configs.put(KafkaConfig.BrokerIdProp(), "1");
//        reporter.configure(configs);
//        assertThatThrownBy(() -> reporter.contextChange(ctxWithCluster))
//            .isInstanceOf(ConfigException.class);
    }

    @Test
    public void testInitConfigsInvalidIntervalConfig() {
//        configs.put(ConfluentTelemetryConfig.COLLECT_INTERVAL_CONFIG, "not-a-number");
//        reporter.configure(configs);
//        assertThatThrownBy(() -> reporter.contextChange(ctxWithCluster)).isInstanceOf(ConfigException.class);
    }


    @Test
    public void testInitConfigsNoExporters() {
        disableDefaultExporters();
        reporter.configure(configs);
        reporter.contextChange(ctx);
//        assertThat(reporter.getExporters()).hasSize(0);
    }

    @Test
    public void testInitNonBrokers() {
        disableDefaultExporters();
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
//            ExporterConfig.ExporterType.kafka.name()
//        );
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
//            "127.0.0.1:9092"
//        );
//        configs.put(ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.ENABLED_CONFIG, "true");
//        reporter.configure(configs);
//        reporter.contextChange(ctx);
//        assertThat(reporter.getCollectors()).isNotEmpty();
    }

    @Test
    public void initKafkaExporterFails() {
        disableDefaultExporters();
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.TYPE_CONFIG,
//            ExporterConfig.ExporterType.kafka.name()
//        );
//        configs.put(ConfluentTelemetryConfig.exporterPrefixForName("test") + ExporterConfig.ENABLED_CONFIG, "true");
//        reporter.configure(configs);
//        assertThatThrownBy(() -> reporter.contextChange(ctxWithCluster)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void initKafkaExporterSuccess() {
        disableDefaultExporters();
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
//            ExporterConfig.ExporterType.kafka.name()
//        );
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
//            "127.0.0.1:9092"
//        );
//        configs.put(ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.ENABLED_CONFIG, "true");
//        reporter.configure(configs);
//        //set cluster id: clusterid
//
//        reporter.contextChange(ctxWithCluster);
//        assertThat(reporter.getExporters())
//            .hasEntrySatisfying("name", new Condition<>(c -> c instanceof KafkaExporter, "is KafkaExporter"));
    }

    @Test
    public void initHttpExporterFails() {
        disableDefaultExporters();
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
//            ExporterConfig.ExporterType.http.name()
//        );
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + HttpExporterConfig.BUFFER_MAX_BATCH_SIZE,
//            "not-a-number"
//        );
//        configs.put(ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.ENABLED_CONFIG, "true");
//        reporter.configure(configs);
//        assertThatThrownBy(() -> reporter.contextChange(ctxWithCluster)).isInstanceOf(ConfigException.class);
    }

    @Test
    public void initHttpExporterSuccess() {
        disableDefaultExporters();
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
//            ExporterConfig.ExporterType.http.name()
//        );
//        configs.put(ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.ENABLED_CONFIG, "true");
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//
//        assertThat(reporter.getExporters())
//            .hasEntrySatisfying("name", new Condition<>(c -> c instanceof HttpExporter, "is HttpExporter"));
    }

    @Test
    public void testReconfigurables() {
//        configs.putAll(
//            ImmutableMap.of(
//                // kafka exporter
//                ConfluentTelemetryConfig.exporterPrefixForName("kafka") + ExporterConfig.TYPE_CONFIG,
//                ExporterConfig.ExporterType.kafka.name(),
//                ConfluentTelemetryConfig.exporterPrefixForName("kafka") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
//                "127.0.0.1:9092",
//                ConfluentTelemetryConfig.exporterPrefixForName("kafka") + KafkaExporterConfig.ENABLED_CONFIG, "true",
//
//                // http exporter
//                ConfluentTelemetryConfig.exporterPrefixForName("http") + ExporterConfig.TYPE_CONFIG,
//                ExporterConfig.ExporterType.http.name(),
//                ConfluentTelemetryConfig.exporterPrefixForName("http") + ExporterConfig.ENABLED_CONFIG, "true"
//            )
//        );
//
//        Set<String> httpExporters = ImmutableSet.of("http", ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME);
//        Set<String> allExporters =
//            new ImmutableSet.Builder<String>()
//                .addAll(httpExporters)
//                .add("kafka")
//                .build();
//
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//
//        Set<String> expectedConfigs = new HashSet<>();
//        expectedConfigs.addAll(ConfluentTelemetryConfig.RECONFIGURABLES);
//
//        // add named http exporter configs
//        for (String name : httpExporters) {
//            for (String configName : HttpExporterConfig.RECONFIGURABLE_CONFIGS) {
//                expectedConfigs.add(
//                    ConfluentTelemetryConfig.exporterPrefixForName(name) + configName
//                );
//            }
//        }
//
//        // add named exporter configs
//        for (String name : allExporters) {
//            for (String configName : ExporterConfig.RECONFIGURABLES) {
//                expectedConfigs.add(
//                    ConfluentTelemetryConfig.exporterPrefixForName(name) + configName
//                );
//            }
//        }
//
//        assertThat(reporter.reconfigurableConfigs())
//            .containsAll(expectedConfigs)
//            .hasSize(expectedConfigs.size());
    }

    @Test
    public void testConfigResourceLabels() {
//        configs.put(ConfluentTelemetryConfig.PREFIX_LABELS + "physical_cluster_id", "pkc-test");
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//        assertThat(MetricsUtils.attributesMap(reporter.getContext().getResource()))
//            .containsKey("physical_cluster_id");
    }

    @Test
    public void testOnRemoteConfigurationReceived() {
        enableDefaultExporters();
//        configs.put(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, "includeNoMetrics");
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//
//        Emitter emitter = reporter.emitter();
//        MetricKey key = new MetricKey("metrics.test", Collections.emptyMap());
//        RemoteConfigurationResponse remoteConf = new RemoteConfigurationResponse(
//            "abc123",
//            new RemoteConfiguration(
//                ImmutableSet.of(new SimpleNamedFilter("test", "metrics\\..*")),
//                ImmutableSet.of("test"), Collections.emptySet())
//        );
//
//        reporter.onRemoteConfigurationReceived(remoteConf.getConfig());
//
//        assertThat(emitter.shouldEmitMetric(key)).isTrue();
    }

    @Test
    public void testOnRemoteConfigurationReceivedCantUpdateDefaultFilter() {
        enableDefaultExporters();
//        configs.put(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, "includeNoMetrics");
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//
//        Emitter emitter = reporter.emitter();
//        MetricKey key = new MetricKey("metrics.test", Collections.emptyMap());
//        RemoteConfigurationResponse remoteConf = new RemoteConfigurationResponse(
//            "abc123",
//            new RemoteConfiguration(
//                ImmutableSet.of(new SimpleNamedFilter("_default", "metrics\\..*")),
//                ImmutableSet.of("_default"), Collections.emptySet())
//        );
//
//        reporter.onRemoteConfigurationReceived(remoteConf.getConfig());
//
//        assertThat(emitter.shouldEmitMetric(key)).isFalse();
    }

    @Test
    public void testReconfigureWithRemoteConfigDisabled() {
        enableDefaultExporters();
//        configs.put(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, "includeNoMetrics");
//        configs.put(ConfluentTelemetryConfig.PREFIX_REMOTE_CONFIG + RemoteConfigConfiguration.ENABLED_CONFIG, false);
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//
//        configs.put(ConfluentTelemetryConfig.METRICS_INCLUDE_CONFIG, ".*");
//        reporter.reconfigure(configs);
//
//        Emitter emitter = reporter.emitter();
//        MetricKey key = new MetricKey("metrics.test", Collections.emptyMap());
//        assertThat(emitter.shouldEmitMetric(key)).isTrue();
    }

    private void disableDefaultExporters() {
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
//                + ExporterConfig.ENABLED_CONFIG,
//            "false"
//        );
    }

    private void enableDefaultExporters() {
//        configs.put(
//            ConfluentTelemetryConfig.exporterPrefixForName(ConfluentTelemetryConfig.EXPORTER_CONFLUENT_NAME)
//                + ExporterConfig.ENABLED_CONFIG,
//            "true"
//        );
    }

    @Test
    public void testReporterId() {
        reporter.configure(configs);
        reporter.contextChange(ctxWithCluster);
//        Map<String, String> tags = reporter.getSelfMetrics().config().tags();
//        assertThat(tags).containsKey(TELEMETRY_REPORTER_ID_TAG);
//        String uuid = tags.get(TELEMETRY_REPORTER_ID_TAG);
//        assertThatCode(() -> Uuid.fromString(uuid)).doesNotThrowAnyException();
//        assertThat(uuid).isEqualTo(reporter.getReporterId().toString());
    }

    @Test
    public void testMbeanCleanup() throws MalformedObjectNameException {
        ClientTelemetryReporter secondReporter = new ClientTelemetryReporter();

        disableDefaultExporters();
//        configs.put(
//                ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.TYPE_CONFIG,
//                ExporterConfig.ExporterType.kafka.name()
//        );
//        configs.put(
//                ConfluentTelemetryConfig.exporterPrefixForName("name") + KafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG,
//                "127.0.0.1:9092"
//        );
//        configs.put(ConfluentTelemetryConfig.exporterPrefixForName("name") + ExporterConfig.ENABLED_CONFIG, "true");
//
//        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
//        final ObjectName mbeanPattern = new ObjectName("confluent.telemetry:type=KafkaExporter,*");
//
//        reporter.configure(configs);
//        reporter.contextChange(ctxWithCluster);
//        Set<ObjectName> mbeanNames = server.queryNames(mbeanPattern, null);
//        assertThat(mbeanNames).size().isOne();
//
//        secondReporter.configure(configs);
//        secondReporter.contextChange(ctxWithCluster);
//        mbeanNames = server.queryNames(mbeanPattern, null);
//        assertThat(mbeanNames).size().isEqualTo(2);
//
//        reporter.close();
//        mbeanNames = server.queryNames(mbeanPattern, null);
//        assertThat(mbeanNames).size().isOne();
//
//        secondReporter.close();
//        mbeanNames = server.queryNames(mbeanPattern, null);
//        assertThat(mbeanNames).size().isZero();
    }
}
