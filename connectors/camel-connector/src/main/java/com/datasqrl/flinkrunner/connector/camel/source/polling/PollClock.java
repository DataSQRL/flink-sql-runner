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
package com.datasqrl.flinkrunner.connector.camel.source.polling;

import org.apache.flink.annotation.Internal;

/**
 * Assigns the timestamp T of each poll or snapshot unit: {@code max(now, lastT + 1ms)}. Seeded from
 * split state, so T stays monotonic across a failover to a TaskManager with a slower clock.
 */
@Internal
public final class PollClock {

  /** No timestamp: no unit has completed yet, or none is in progress. */
  public static final long NONE = Long.MIN_VALUE;

  private long lastT;

  public PollClock(long lastT) {
    this.lastT = lastT;
  }

  /** The timestamp of a new unit. */
  public long next() {
    lastT = Math.max(System.currentTimeMillis(), lastT + 1);
    return lastT;
  }

  /** Accounts for a unit that reuses an earlier timestamp, such as one resumed after a restore. */
  public void observe(long t) {
    lastT = Math.max(lastT, t);
  }
}
