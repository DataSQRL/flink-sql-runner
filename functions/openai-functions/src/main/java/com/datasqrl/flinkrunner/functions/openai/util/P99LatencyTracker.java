/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class P99LatencyTracker {

  private static final int DEFAULT_MAX_SIZE = 100; // Default size of the sliding window

  private final List<Long> latencies; // Stores the recorded latencies
  private final int maxSize; // Maximum size of the sliding window

  public P99LatencyTracker() {
    this(DEFAULT_MAX_SIZE);
  }

  public P99LatencyTracker(int maxSize) {
    this.maxSize = maxSize;
    this.latencies = new ArrayList<>();
  }

  public void recordLatency(long latencyMs) {
    latencies.add(latencyMs);

    // If the size exceeds maxSize, remove the oldest entry
    if (latencies.size() > maxSize) {
      latencies.remove(0); // Remove the oldest entry
    }
  }

  public Long getP99Latency() {
    if (latencies.isEmpty()) {
      return 0L;
    }

    // Sort the latencies to find the P99 value
    Collections.sort(latencies);
    int index = (int) Math.ceil(0.99 * latencies.size()) - 1; // P99 index
    return latencies.get(index);
  }
}
