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

/** Spaces out Camel calls to at most a given number per minute. Used on the fetcher thread only. */
@Internal
public final class RateLimiter {

  private final long minIntervalMs;
  private long lastCallAt = Long.MIN_VALUE;

  /**
   * @param perMinute max calls per minute, or 0 for no limit
   */
  public RateLimiter(int perMinute) {
    this.minIntervalMs = perMinute > 0 ? Math.max(1, 60_000L / perMinute) : 0;
  }

  /** Waits until the next call is allowed. */
  public void acquire() {
    if (minIntervalMs == 0) {
      return;
    }
    long wait =
        lastCallAt == Long.MIN_VALUE ? 0 : lastCallAt + minIntervalMs - System.currentTimeMillis();
    if (wait > 0) {
      try {
        Thread.sleep(wait);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    lastCallAt = System.currentTimeMillis();
  }
}
