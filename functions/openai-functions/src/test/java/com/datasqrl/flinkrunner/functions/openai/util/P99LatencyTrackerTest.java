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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class P99LatencyTrackerTest {

  private P99LatencyTracker tracker;

  @BeforeEach
  public void setUp() {
    // Set up a new tracker with a max size of 100
    tracker = new P99LatencyTracker(100);
  }

  @Test
  public void testP99LatencyWithLessThan100Entries() {
    tracker.recordLatency(100);
    tracker.recordLatency(200);
    tracker.recordLatency(300);
    tracker.recordLatency(400);
    tracker.recordLatency(500);

    // The P99 latency should be the maximum value since we have less than 100 entries
    assertThat(tracker.getP99Latency()).isEqualTo(500);
  }

  @Test
  public void testP99LatencyWithExactly100Entries() {
    for (int i = 1; i <= 100; i++) {
      tracker.recordLatency(i);
    }

    // The P99 latency should be the 99th largest value, which is 99 in this case
    assertThat(tracker.getP99Latency()).isEqualTo(99);
  }

  @Test
  public void testP99LatencyWithMoreThan100Entries() {
    for (int i = 1; i <= 120; i++) {
      tracker.recordLatency(i);
    }

    // The P99 latency should reflect the highest 99% of the last 100 records
    assertThat(tracker.getP99Latency()).isEqualTo(119);
  }

  @Test
  public void testP99LatencyWithEmptyTracker() {
    // When no latencies have been recorded, P99 should return 0
    assertThat(tracker.getP99Latency()).isZero();
  }

  @Test
  public void testSlidingWindowBehavior() {
    // Add 5 latencies
    tracker.recordLatency(100);
    tracker.recordLatency(200);
    tracker.recordLatency(300);
    tracker.recordLatency(400);
    tracker.recordLatency(500);

    // Initial P99 should be 500
    assertThat(tracker.getP99Latency()).isEqualTo(500);

    // Now add more latencies to exceed the max size (e.g., 101)
    for (int i = 600; i <= 700; i++) {
      tracker.recordLatency(i);
    }

    // The oldest latencies (100-500) should have been removed,
    // Now the highest 99% of the new list should be considered
    assertThat(tracker.getP99Latency()).isEqualTo(699);
  }
}
