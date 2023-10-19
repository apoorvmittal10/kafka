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

package org.apache.kafka.common.telemetry;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.requests.AbstractRequest.Builder;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionsResponse;
import org.apache.kafka.common.requests.PushTelemetryResponse;

import java.util.Optional;

/**
 * The interface used by the `NetworkClient` to send telemetry requests.
 */
public interface ClientTelemetrySender extends AutoCloseable {

    /**
     * Return the next time when the telemetry API should be attempted (i.e., interval time has elapsed).
     * <p>
     * If another telemetry API is in-flight, then {@code timeoutMs} should be returned as the
     * maximum wait time.
     *
     * @param timeoutMs The timeout for the inflight telemetry API call.
     * @return remaining time in ms till the telemetry API be attempted again.
     */
    long timeToNextUpdate(long timeoutMs);

    /**
     * Return the telemetry request based on client state i.e. determine if
     * {@link org.apache.kafka.common.requests.GetTelemetrySubscriptionsRequest} or
     * {@link org.apache.kafka.common.requests.PushTelemetryRequest} be constructed.
     *
     * @return request for telemetry API call.
     */
    Optional<Builder<?>> createRequest();

    /**
     * Handle successful response for get telemetry subscriptions request.
     *
     * @param response subscriptions telemetry API response
     */
    void handleResponse(GetTelemetrySubscriptionsResponse response);

    /**
     * Handle successful response for push telemetry request.
     *
     * @param response push telemetry API response
     */
    void handleResponse(PushTelemetryResponse response);

    /**
     * Handle response for failed get telemetry subscriptions request.
     *
     * @param kafkaException the fatal exception.
     */
    void handleFailedGetTelemetrySubscriptionsRequest(KafkaException kafkaException);

    /**
     * Handle response for failed push telemetry request.
     *
     * @param kafkaException the fatal exception.
     */
    void handleFailedPushTelemetryRequest(KafkaException kafkaException);
}