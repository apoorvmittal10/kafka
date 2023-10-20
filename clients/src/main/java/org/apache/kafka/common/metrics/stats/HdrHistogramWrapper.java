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
package org.apache.kafka.common.metrics.stats;

import java.util.ArrayList;
import java.util.List;
import org.HdrHistogram.DoubleHistogram;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A Histogram class of CompoundStat class using HdrHistogram. See {@link org.HdrHistogram.Histogram} for the histogram implementation
 */
public class HdrHistogramWrapper implements CompoundStat {

  private final MetricName metricName;
  private final DoubleHistogram hdrHistogram;

  public HdrHistogramWrapper(MetricName metricName) {
    this.metricName = metricName;
    this.hdrHistogram = new DoubleHistogram(5);
  }

  @Override
  public List<NamedMeasurable> stats() {
    List<NamedMeasurable> ms = new ArrayList<>();

    ms.add(new NamedMeasurable(new MetricName(
          metricName.name() + "-p25",
          metricName.group(),
          metricName.description(),
          metricName.tags()), (config, now) -> {
            System.out.println("[APM] - Basic Percentile Distribution");
            hdrHistogram.outputPercentileDistribution(System.out, 1.0);
            return hdrHistogram.getValueAtPercentile(0.25);
    }));

    ms.add(new NamedMeasurable(new MetricName(
        metricName.name() + "-p50",
        metricName.group(),
        metricName.description(),
        metricName.tags()), (config, now) -> hdrHistogram.getValueAtPercentile(0.50)));

    ms.add(new NamedMeasurable(new MetricName(
        metricName.name() + "-p90",
        metricName.group(),
        metricName.description(),
        metricName.tags()), (config, now) -> hdrHistogram.getValueAtPercentile(0.90)));

    ms.add(new NamedMeasurable(new MetricName(
        metricName.name() + "-p95",
        metricName.group(),
        metricName.description(),
        metricName.tags()), (config, now) -> hdrHistogram.getValueAtPercentile(0.95)));

    ms.add(new NamedMeasurable(new MetricName(
        metricName.name() + "-p99",
        metricName.group(),
        metricName.description(),
        metricName.tags()), (config, now) -> hdrHistogram.getValueAtPercentile(0.99)));

    return ms;
  }

  @Override
  public void record(MetricConfig config, double value, long timeMs) {
    this.hdrHistogram.recordValue(value);
  }

}