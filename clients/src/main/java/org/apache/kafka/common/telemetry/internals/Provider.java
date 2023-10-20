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
package org.apache.kafka.common.telemetry.internals;

import io.opentelemetry.proto.resource.v1.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.MetricsContext;

/**
 * Implement this interface to collect metrics for your component.
 */
public interface Provider extends Configurable {

  /**
   * Validate that all the data required for generating correct metrics is present. The provider
   * will be disabled if validation fails.
   *
   * @param metricsContext {@link MetricsContext}
   * @return false if all the data required for generating correct metrics is missing, true
   * otherwise.
   */
  boolean validate(MetricsContext metricsContext, Map<String, ?> config);

  /**
   * Domain of the active provider. This is used by other parts of the reporter.
   *
   * @return Domain in string format.
   */
  String domain();

  /**
   * The resource for this provider.
   *
   * @return A fully formed {@link Resource} will all the tags.
   */
  Resource resource();

  /**
   * Sets the metrics labels for the service or library exposing metrics. This will be called before {@link org.apache.kafka.common.metrics.MetricsReporter#init(List)} and may be called anytime after that.
   *
   * @param metricsContext {@link MetricsContext}
   */
  void contextChange(MetricsContext metricsContext);

  /**
   * The collector for Kafka Metrics library is enabled by default. If you need any more, add them
   * here.
   *
   * @param ctx {@link Context}
   * @return List of extra collectors
   */
  default List<MetricsCollector> extraCollectors(Context ctx) {
    return Collections.emptyList();
  }
}
