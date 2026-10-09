/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner.stdlib.typesafe;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

/** The per-subtask metrics of {@link typesafe_classify}. */
class ClassifyMetrics {

  static final String ERROR_CLASS_TAG = "error_class";

  Counter requests;
  Histogram requestLatency;
  Counter retries;
  Counter nullRows;
  Counter inputTokens;
  Counter outputTokens;

  private final Map<ClassifyException.ErrorClass, Counter> errors =
      new EnumMap<>(ClassifyException.ErrorClass.class);

  ClassifyMetrics(MetricGroup group) {
    requests = group.counter("requests");
    requestLatency = group.histogram("requestLatency", new SlidingWindowHistogram(1024));
    retries = group.counter("retries");
    nullRows = group.counter("nullRows");
    inputTokens = group.counter("inputTokens");
    outputTokens = group.counter("outputTokens");

    for (var c : ClassifyException.ErrorClass.values()) {
      errors.put(c, group.addGroup(ERROR_CLASS_TAG, c.getTag()).counter("errors"));
    }
  }

  /** Metrics that are not reported anywhere, for use outside of a Flink task. */
  static ClassifyMetrics unregistered() {
    return new ClassifyMetrics(new UnregisteredMetricsGroup());
  }

  Counter errors(ClassifyException.ErrorClass errorClass) {
    return errors.get(errorClass);
  }

  /** A histogram over the most recent samples, avoiding a dependency on flink-runtime. */
  static class SlidingWindowHistogram implements Histogram {

    private final long[] samples;
    private long count;

    SlidingWindowHistogram(int windowSize) {
      this.samples = new long[windowSize];
    }

    @Override
    public synchronized void update(long value) {
      samples[(int) (count % samples.length)] = value;
      count++;
    }

    @Override
    public synchronized long getCount() {
      return count;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
      long[] window = Arrays.copyOf(samples, (int) Math.min(count, samples.length));
      Arrays.sort(window);

      return new Statistics(window);
    }
  }

  @RequiredArgsConstructor
  private static class Statistics extends HistogramStatistics {

    private final long[] sorted;

    @Override
    public double getQuantile(double quantile) {
      if (sorted.length == 0) {
        return 0;
      }
      var idx = (int) Math.ceil(quantile * sorted.length) - 1;
      return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
    }

    @Override
    public long[] getValues() {
      return sorted.clone();
    }

    @Override
    public int size() {
      return sorted.length;
    }

    @Override
    public double getMean() {
      return Arrays.stream(sorted).average().orElse(0);
    }

    @Override
    public double getStdDev() {
      if (sorted.length == 0) {
        return 0;
      }
      var mean = getMean();
      var variance =
          Arrays.stream(sorted).mapToDouble(v -> (v - mean) * (v - mean)).sum() / sorted.length;
      return Math.sqrt(variance);
    }

    @Override
    public long getMax() {
      return sorted.length == 0 ? 0 : sorted[sorted.length - 1];
    }

    @Override
    public long getMin() {
      return sorted.length == 0 ? 0 : sorted[0];
    }
  }
}
