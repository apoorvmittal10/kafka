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
package org.apache.kafka.clients;

import static org.apache.kafka.common.telemetry.internals.Utils.notEmptyString;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;
import java.util.Map;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.telemetry.internals.MetricsProvider;
import org.apache.kafka.common.telemetry.internals.Utils;

public class ClientTelemetryProvider implements MetricsProvider {

    // Visible for testing
    public static final String DOMAIN = "org.apache.kafka";

    // Client metrics tags
    public static final String LABEL_CLIENT_ID = "client_id";
    public static final String LABEL_NODE_ID = "node_id";
    public static final String CLIENT_RACK = "client_rack";
    public static final String GROUP_ID = "group_id";
    public static final String GROUP_INSTANCE_ID = "group_instance_id";
    public static final String GROUP_MEMBER_ID = "group_member_id";
    public static final String TRANSACTIONAL_ID = "transactional_id";

    private Resource resource;
    private Map<String, ?> config;

    @Override
    public synchronized void configure(Map<String, ?> configs) {
        // System.out.println("[APM] configure: " + configs);
        this.config = configs;
    }

    @Override
    public boolean validate(MetricsContext metricsContext, Map<String, ?> config) {
        // System.out.println("[APM] - validate client metrics context. Context labels: " + metricsContext.contextLabels() + " config: " + config);
        // metric collection will be disabled for clients without a client id (e.g. transient admin clients)
        return notEmptyString(config, CommonClientConfigs.CLIENT_ID_CONFIG) &&
            validateRequiredLabels(metricsContext.contextLabels());
    }

    @Override
    public void contextChange(MetricsContext metricsContext) {
        // System.out.println("[APM] - Context change. Context labels: " + metricsContext.contextLabels());

        final Resource.Builder resourceBuilder = Resource.newBuilder();
        addAttribute(resourceBuilder, LABEL_CLIENT_ID,
            (String) this.config.get(CommonClientConfigs.CLIENT_ID_CONFIG));

        this.resource = resourceBuilder.build();
    }

    @Override
    public Resource resource() {
        return this.resource;
    }

    @Override
    public String domain() {
        return DOMAIN;
    }

    private  boolean validateRequiredLabels(Map<String, String> metadata) {
        return Utils.validateRequiredResourceLabels(metadata);
    }

    // TODO (Apoorv): May be prefix with namespace
    private void addAttribute(Resource.Builder resourceBuilder, String key, String value) {
        final KeyValue.Builder kv = KeyValue.newBuilder()
            .setKey(key)
            .setValue(AnyValue.newBuilder().setStringValue(value));
        resourceBuilder.addAttributes(kv);
    }
}
