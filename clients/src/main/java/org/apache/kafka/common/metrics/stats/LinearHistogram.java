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
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * A Histogram class of CompoundStat class using Linear BinScheme. See {@link Histogram} for the histogram implementation
 */
public class LinearHistogram extends HistogramStat implements CompoundStat {
  public LinearHistogram(int numBins, double max, MetricName metricName) {
    super(new LinearBinScheme(numBins, max), metricName);
  }

  @Override
  public List<NamedMeasurable> stats() {
    List<NamedMeasurable> ms = new ArrayList<>();
    for (int i = 0; i < getBuckets().size(); i++) {
      final int finalI = i;
      ms.add(new NamedMeasurable(getBuckets().get(i).name(), (config, now) -> {
        return value(config, now, finalI);
      }));
    }
    return ms;
  }

  @Override
  public void record(MetricConfig config, double value, long timeMs) {
    super.record(value);
  }

  public double value(MetricConfig config, long now, int index) {
    return counts()[index];
  }
}