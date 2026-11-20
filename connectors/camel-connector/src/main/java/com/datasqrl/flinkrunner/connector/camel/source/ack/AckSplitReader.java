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
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import com.datasqrl.flinkrunner.connector.camel.source.RecordContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.camel.Exchange;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs on the single fetcher thread: receives exchanges through the ConsumerTemplate and maps them.
 *
 * <p>Each fetch blocks for at most the receive timeout, then drains whatever else is ready without
 * waiting. Receiving pauses while {@code maxPending} exchanges are waiting for a checkpoint, and
 * while the runtime has to commit what it received before it can take more.
 */
final class AckSplitReader<OUT, R> implements SplitReader<AckRecord<R>, AckSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(AckSplitReader.class);

  private static final int MAX_BATCH = 500;
  private static final int MAX_LOGGED_PAYLOAD = 500;
  private static final long MAX_PAUSE_MS = 100;
  // An exchange is one raw record.
  private static final long EXCHANGE_ROW = 1;

  private final AckSettings<OUT, R> settings;
  private final CamelRuntime runtime;
  private final Counter droppedRecords;
  private final Counter failedRecords;

  private AckSplit split;
  private int emptyPolls;
  private long lastPollTimestamp = Long.MIN_VALUE;
  private boolean warnedFull;

  AckSplitReader(
      AckSettings<OUT, R> settings,
      CamelRuntime runtime,
      Counter droppedRecords,
      Counter failedRecords) {
    this.settings = settings;
    this.runtime = runtime;
    this.droppedRecords = droppedRecords;
    this.failedRecords = failedRecords;
  }

  @Override
  public RecordsWithSplitIds<AckRecord<R>> fetch() throws IOException {
    if (split == null) {
      return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
    }

    int capacity = settings.maxPending() - runtime.unacked();
    if (capacity <= 0) {
      return waitForAcks();
    }
    warnedFull = false;
    if (runtime.receivePaused()) {
      return pause();
    }

    long timeoutMs = settings.receiveTimeout().toMillis();
    List<AckRecord<R>> records = new ArrayList<>();
    Exchange exchange = runtime.receiveWithRetries(timeoutMs);
    long pollTimestamp = exchange != null ? nextPollTimestamp() : 0;
    while (exchange != null) {
      records.add(new AckRecord<>(convert(exchange, pollTimestamp), exchange, pollTimestamp));
      if (records.size() >= Math.min(capacity, MAX_BATCH) || runtime.receivePaused()) {
        break;
      }
      exchange = runtime.receiveNoWait(settings.endpointUri());
    }
    if (!records.isEmpty() && settings.pollWatermarks()) {
      records.add(AckRecord.endOfPoll(pollTimestamp));
    }

    emptyPolls = records.isEmpty() ? emptyPolls + 1 : 0;
    Set<String> finished =
        settings.isBounded() && emptyPolls >= settings.stopAfterEmptyPolls()
            ? Set.of(split.splitId())
            : Set.of();
    if (!finished.isEmpty()) {
      LOG.info(
          "No exchanges from {} after {} receives, finishing", settings.endpointUri(), emptyPolls);
      split = null;
    }
    return new RecordsBySplits<>(
        records.isEmpty() ? Map.of() : Map.of(CamelSplitEnumerator.SPLIT_ID, records), finished);
  }

  /** Wall-clock time, kept strictly increasing so every poll has its own timestamp. */
  private long nextPollTimestamp() {
    lastPollTimestamp = Math.max(System.currentTimeMillis(), lastPollTimestamp + 1);
    return lastPollTimestamp;
  }

  private R convert(Exchange exchange, long pollTimestamp) {
    try {
      OUT value = settings.source().map(exchange);
      if (value == null) {
        droppedRecords.inc();
        return null;
      }
      return settings
          .converter()
          .convert(value, new RecordContext(pollTimestamp, exchange, EXCHANGE_ROW));
    } catch (Exception e) {
      failedRecords.inc();
      LOG.warn(
          "Skipping exchange {} from {} that failed to map or convert. Payload: {}",
          exchange.getExchangeId(),
          settings.endpointUri(),
          truncatedPayload(exchange),
          e);
      return null;
    }
  }

  private RecordsWithSplitIds<AckRecord<R>> waitForAcks() {
    if (!warnedFull) {
      LOG.warn(
          "{} exchanges are waiting for a checkpoint, pausing receive. "
              + "Is checkpointing enabled?",
          runtime.unacked());
      warnedFull = true;
    }
    return pause();
  }

  /** Waits a moment without receiving. Not an empty poll: the endpoint was never asked. */
  private RecordsWithSplitIds<AckRecord<R>> pause() {
    try {
      Thread.sleep(Math.min(MAX_PAUSE_MS, settings.receiveTimeout().toMillis()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
  }

  private static String truncatedPayload(Exchange exchange) {
    try {
      String body = exchange.getMessage().getBody(String.class);
      if (body == null || body.length() <= MAX_LOGGED_PAYLOAD) {
        return body;
      }
      return body.substring(0, MAX_LOGGED_PAYLOAD) + "...";
    } catch (Exception e) {
      return "<unreadable: " + e + ">";
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<AckSplit> splitsChanges) {
    if (!(splitsChanges instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          "Unsupported splits change " + splitsChanges.getClass());
    }
    for (AckSplit added : splitsChanges.splits()) {
      split = added;
    }
  }

  @Override
  public void wakeUp() {
    // fetch() blocks for at most the receive timeout.
  }

  @Override
  public void close() {
    // The CamelContext belongs to the reader, which outlives this fetcher.
  }
}
