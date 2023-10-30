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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kafka.common.telemetry.internals.MetricKeyable;
import org.apache.kafka.common.telemetry.internals.MetricsEmitter;
import org.apache.kafka.common.telemetry.internals.SinglePointMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTelemetryEmitter implements MetricsEmitter {

    private final static Logger log = LoggerFactory.getLogger(ClientTelemetryEmitter.class);

    private final Predicate<? super MetricKeyable> selector;
    private final ClientTelemetryContext context;
    private final List<SinglePointMetric> emitted;

    public ClientTelemetryEmitter(Predicate<? super MetricKeyable> selector, ClientTelemetryContext context) {
        this.selector = selector;
        this.context = context;
        this.emitted = new ArrayList<>();
    }

    @Override
    public boolean shouldEmitMetric(MetricKeyable metricKeyable) {
        log.info("[APM] - metricKeyable: " + metricKeyable.key());
        return true;
//        return selector.test(metricKeyable);
    }

    @Override
    public boolean emitMetric(SinglePointMetric metric) {
        emitted.add(metric);
        return true;
    }

    @Override
    public List<SinglePointMetric> emittedMetrics() {
        return Collections.unmodifiableList(emitted);
    }
}
