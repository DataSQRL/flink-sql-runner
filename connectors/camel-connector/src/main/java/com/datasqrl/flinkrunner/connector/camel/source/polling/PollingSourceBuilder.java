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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/** Options that the cursor and file mode builders share. */
public abstract class PollingSourceBuilder<B extends PollingSourceBuilder<B>> {

  private PollSchedule pollSchedule = PollSchedule.every(Duration.ofMinutes(5));
  private Duration fetchTimeout = Duration.ofSeconds(1);
  private boolean bounded;
  private boolean pollWatermarks;
  private int rateLimitPerMinute;
  private int retryMaxAttempts;
  private Duration retryInitialBackoff = Duration.ofMinutes(1);
  private Duration retryMaxBackoff = Duration.ofMinutes(30);
  private Map<String, String> componentOptions = Map.of();

  protected PollingSourceBuilder() {}

  @SuppressWarnings("unchecked")
  private B self() {
    return (B) this;
  }

  /** Time between polls once a poll is complete. Defaults to 5 minutes. */
  public B pollInterval(Duration pollInterval) {
    return pollSchedule(PollSchedule.every(pollInterval));
  }

  /**
   * When the next poll starts once a poll is complete, such as {@link PollSchedule#cron}. The first
   * poll starts right away. Defaults to every 5 minutes.
   */
  public B pollSchedule(PollSchedule pollSchedule) {
    this.pollSchedule = Objects.requireNonNull(pollSchedule, "pollSchedule");
    return self();
  }

  /**
   * How long one Camel receive waits for an exchange, and how long the fetcher waits at most
   * between checks while idle or backing off. Defaults to 1s.
   */
  public B fetchTimeout(Duration fetchTimeout) {
    this.fetchTimeout = fetchTimeout;
    return self();
  }

  /**
   * Makes the source bounded: it finishes after one complete poll, ignoring the poll interval. By
   * default the source is unbounded.
   */
  public B bounded(boolean bounded) {
    this.bounded = bounded;
    return self();
  }

  /**
   * Emits a watermark at a unit's timestamp once the unit completes with new rows, then marks the
   * source idle until the next unit. Off by default.
   */
  public B pollWatermarks(boolean pollWatermarks) {
    this.pollWatermarks = pollWatermarks;
    return self();
  }

  /** Max Camel calls (requests, listings) per minute. No limit by default. */
  public B rateLimit(int perMinute) {
    this.rateLimitPerMinute = perMinute;
    return self();
  }

  /**
   * Retries failed fetches with exponential backoff from {@code initialBackoff} up to {@code
   * maxBackoff}, and fails the job after {@code maxAttempts} failures in a row. Off by default: a
   * failed request then fails the job straight away, and a failed Camel poll is logged by Camel and
   * polled again.
   */
  public B retry(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    this.retryMaxAttempts = maxAttempts;
    this.retryInitialBackoff = initialBackoff;
    this.retryMaxBackoff = maxBackoff;
    return self();
  }

  /**
   * Camel component options, keyed {@code <component>.<option>} such as {@code
   * salesforce.clientId}. The component name ends at the first dot. An unknown option fails the
   * reader.
   */
  public B componentOptions(Map<String, String> componentOptions) {
    this.componentOptions = Map.copyOf(componentOptions);
    return self();
  }

  protected Map<String, String> componentOptions() {
    return componentOptions;
  }

  protected PollSettings pollSettings() {
    requirePositive(fetchTimeout, "fetchTimeout");
    if (rateLimitPerMinute < 0) {
      throw new IllegalArgumentException("rateLimit must not be negative");
    }
    if (retryMaxAttempts < 0) {
      throw new IllegalArgumentException("retry maxAttempts must not be negative");
    }
    return new PollSettings(
        pollSchedule,
        fetchTimeout,
        bounded,
        pollWatermarks,
        rateLimitPerMinute,
        retryMaxAttempts,
        retryInitialBackoff,
        retryMaxBackoff);
  }

  private static void requirePositive(Duration duration, String name) {
    if (duration == null || duration.isNegative() || duration.isZero()) {
      throw new IllegalArgumentException(name + " must be positive, got " + duration);
    }
  }
}
