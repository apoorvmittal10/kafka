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

import java.util.Optional;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest.Builder;

/**
 * A {@link MetricsReporter} may implement this interface to indicate support for sending client telemetry
 * to the broker.
 */
@InterfaceStability.Evolving
public interface ClientTelemetrySender extends AutoCloseable {

  long timeToNextUpdate(long timeoutMs);

  Optional<Builder<?>> createRequest();

  void handleResponse(GetTelemetrySubscriptionsResponseData data);

  void handleResponse(PushTelemetryResponseData data);

  void handleFailedRequest(ApiKeys apiKey, KafkaException kafkaException);

}
