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
package com.datasqrl.flinkrunner.connector.camel.source;

import java.io.IOException;
import java.time.Duration;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.apache.camel.util.URISupport;
import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes Camel's fetch errors visible to the fetcher, which otherwise only sees {@code receive} time
 * out: a scheduled poll consumer hands a failed poll to its poll strategy and exception handler,
 * which log it and poll again forever.
 *
 * <p>Installed as the endpoint's {@link PollingConsumerPollStrategy} for scheduled poll consumers,
 * and as its {@link ExceptionHandler} for event-driven ones. Exceptions thrown by {@code receive}
 * itself (JMS, Spring RabbitMQ) are recorded by the fetcher. After a failure the next attempt waits
 * an exponential backoff: {@link #begin} skips polls until then. The fetcher fails the job once
 * {@code maxAttempts} attempts in a row have failed. A successful poll, even an empty one, or a
 * received exchange resets the count.
 */
@Internal
public final class PollErrors implements PollingConsumerPollStrategy, ExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PollErrors.class);

  private final String endpointUri;
  private final int maxAttempts;
  private final Duration initialBackoff;
  private final Duration maxBackoff;

  private int consecutiveFailures;
  private Throwable lastFailure;
  private long nextAttemptAt;

  public PollErrors(
      String endpointUri, int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
    this.endpointUri = URISupport.sanitizeUri(endpointUri);
    this.maxAttempts = maxAttempts;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
  }

  /** Called by Camel before each scheduled poll. Skips polls while backing off. */
  @Override
  public synchronized boolean begin(Consumer consumer, Endpoint endpoint) {
    return System.currentTimeMillis() >= nextAttemptAt;
  }

  @Override
  public synchronized void commit(Consumer consumer, Endpoint endpoint, int polledMessages) {
    succeeded();
  }

  /** Returns false so Camel doesn't retry within the same poll; the backoff decides when. */
  @Override
  public boolean rollback(Consumer consumer, Endpoint endpoint, int retryCounter, Exception cause) {
    failed(cause);
    return false;
  }

  @Override
  public void handleException(Throwable exception) {
    failed(exception);
  }

  @Override
  public void handleException(String message, Throwable exception) {
    failed(exception);
  }

  @Override
  public void handleException(String message, Exchange exchange, Throwable exception) {
    failed(exception);
  }

  /** Whether the fetcher may call {@code receive} again after it threw. */
  synchronized boolean attemptAllowed() {
    return System.currentTimeMillis() >= nextAttemptAt;
  }

  synchronized void succeeded() {
    if (consecutiveFailures > 0) {
      LOG.info("{} recovered after {} failed attempts", endpointUri, consecutiveFailures);
    }
    consecutiveFailures = 0;
    lastFailure = null;
    nextAttemptAt = 0;
  }

  /**
   * Records an exception thrown by {@code receive}, unless the poll strategy already recorded it:
   * file, FTP and SFTP endpoints poll on the calling thread and rethrow the failed poll.
   */
  synchronized void receiveFailed(Throwable cause) {
    if (lastFailure != null && (cause == lastFailure || cause.getCause() == lastFailure)) {
      return;
    }
    failed(cause);
  }

  synchronized void failed(Throwable cause) {
    consecutiveFailures++;
    lastFailure = cause;
    long backoff =
        Math.min(
            maxBackoff.toMillis(),
            initialBackoff.toMillis() << Math.min(consecutiveFailures - 1, 30));
    nextAttemptAt = System.currentTimeMillis() + backoff;
    LOG.warn(
        "Fetch from {} failed (attempt {} of {}), retrying in {} ms",
        endpointUri,
        consecutiveFailures,
        maxAttempts,
        backoff,
        cause);
  }

  synchronized int consecutiveFailures() {
    return consecutiveFailures;
  }

  /** Called on the fetcher thread. Throwing fails the source reader, and with it the job. */
  synchronized void checkAttempts() throws IOException {
    if (consecutiveFailures >= maxAttempts) {
      throw new IOException(
          "Giving up on "
              + endpointUri
              + " after "
              + consecutiveFailures
              + " failed attempts in a row",
          lastFailure);
    }
  }
}
