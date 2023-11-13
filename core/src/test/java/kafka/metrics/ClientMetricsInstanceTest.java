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

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsInstanceTest {

    private Uuid uuid;
    private ClientMetricsInstanceMetadata instanceMetadata;
    private ClientMetricsInstance clientInstance;

    @BeforeEach
    public void setUp() throws UnknownHostException {
        uuid = Uuid.randomUuid();
        instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContext());
        clientInstance = new ClientMetricsInstance(Uuid.randomUuid(), instanceMetadata, 0, 0,
            null, ClientMetricsConfigs.DEFAULT_INTERVAL_MS);
    }

    @Test
    public void testMaybeUpdateRequestEpochValid() {
        // First request should be accepted.
        assertTrue(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
        assertTrue(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestAfterElapsedTimeValid() throws InterruptedException {
        ClientMetricsInstance clientInstance = new ClientMetricsInstance(uuid, instanceMetadata, 0, 0,
            null, 2);
        assertTrue(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
        // sleep for 3 ms to ensure that the next request is accepted.
        Thread.sleep(3);
        // Second request should be accepted as time since last request is greater than the retry interval.
        assertTrue(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
        // Second request should be rejected as time since last request is less than the retry interval.
        assertFalse(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestAfterElapsedTimeValid() throws InterruptedException {
        ClientMetricsInstance clientInstance = new ClientMetricsInstance(uuid, instanceMetadata, 0, 0,
            null, 2);
        assertTrue(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
        // sleep for 3 ms to ensure that the next request is accepted.
        Thread.sleep(3);
        // Second request should be accepted as time since last request is greater than the retry interval.
        assertTrue(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestWithImmediateRetryAfterPushFail() {
        assertTrue(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
        // Next request after push should be rejected as time since last request is less than the retry interval.
        assertFalse(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
        // Second request should be rejected as time since last request is less than the retry interval.
        assertFalse(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestWithImmediateRetryAfterGetValid()
        throws InterruptedException {
        ClientMetricsInstance clientInstance = new ClientMetricsInstance(uuid, instanceMetadata, 0, 0,
            null, 2);
        assertTrue(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
        Thread.sleep(3);
        assertTrue(clientInstance.maybeUpdateGetRequestEpoch(System.currentTimeMillis()));
        // Next request after get should be accepted.
        assertTrue(clientInstance.maybeUpdatePushRequestEpoch(System.currentTimeMillis()));
    }
}
