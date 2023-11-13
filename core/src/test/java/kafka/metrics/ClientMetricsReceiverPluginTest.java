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
package kafka.metrics;

import kafka.metrics.ClientMetricsTestUtils.TestClientMetricsReceiver;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsReceiverPluginTest {

    TestClientMetricsReceiver telemetryReceiver = new TestClientMetricsReceiver();

    @Test
    public void testExportMetrics() throws UnknownHostException {
        assertTrue(ClientMetricsReceiverPlugin.instance().isEmpty());

        ClientMetricsReceiverPlugin.instance().add(telemetryReceiver);
        assertFalse(ClientMetricsReceiverPlugin.instance().isEmpty());

        assertEquals(0, telemetryReceiver.exportMetricsInvokedCount);
        assertTrue(telemetryReceiver.metricsData.isEmpty());

        ClientMetricsReceiverPlugin.instance().exportMetrics(ClientMetricsTestUtils.requestContext(),
            new PushTelemetryRequest.Builder(
                new PushTelemetryRequestData()
                    .setMetrics("test-metrics".getBytes(StandardCharsets.UTF_8)))
                .build());

        assertEquals(1, telemetryReceiver.exportMetricsInvokedCount);
        assertEquals(1, telemetryReceiver.metricsData.size());
        assertEquals("test-metrics", new String(
            telemetryReceiver.metricsData.get(0).array()));
    }
}
