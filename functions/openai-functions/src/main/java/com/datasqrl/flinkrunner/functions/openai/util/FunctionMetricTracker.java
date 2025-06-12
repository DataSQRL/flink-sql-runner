/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.flinkrunner.functions.openai.util;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.functions.FunctionContext;

public class FunctionMetricTracker {

  public static final String P99_METRIC = "com.datasqrl.openai.%s.p99";
  public static final String CALL_COUNT = "com.datasqrl.openai.%s.callCount";
  public static final String ERROR_COUNT = "com.datasqrl.openai.%s.errorCount";

  private final P99LatencyTracker latencyTracker = new P99LatencyTracker();
  private final Counter callCount;
  private final Counter errorCount;

  public FunctionMetricTracker(FunctionContext context, String functionName) {
    final String p99MetricName = String.format(P99_METRIC, functionName);
    final String callCountName = String.format(CALL_COUNT, functionName);
    final String errorCountName = String.format(ERROR_COUNT, functionName);

    context.getMetricGroup().gauge(p99MetricName, (Gauge<Long>) latencyTracker::getP99Latency);
    callCount = context.getMetricGroup().counter(callCountName);
    errorCount = context.getMetricGroup().counter(errorCountName);
  }

  public void increaseCallCount() {
    callCount.inc();
  }

  public void increaseErrorCount() {
    errorCount.inc();
  }

  public void recordLatency(long latencyMs) {
    latencyTracker.recordLatency(latencyMs);
  }
}
