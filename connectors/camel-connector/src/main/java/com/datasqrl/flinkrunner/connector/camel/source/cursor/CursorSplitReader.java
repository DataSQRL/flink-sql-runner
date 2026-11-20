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
package com.datasqrl.flinkrunner.connector.camel.source.cursor;

import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.Request;
import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.RecordContext;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;
import com.datasqrl.flinkrunner.connector.camel.source.polling.RateLimiter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.apache.camel.Exchange;
import org.apache.camel.support.MessageHelper;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs cursor mode on the single fetcher thread. Each fetch sends one request through the
 * ProducerTemplate and hands that page's mapped records to the emitter.
 *
 * <p>A poll starts with {@code request(position)} and follows {@code next(response)} page by page.
 * After the last page, a poll whose position advanced requests again straight away; otherwise it is
 * complete, and the next one starts after the poll interval. In full-refresh mode every poll pages
 * through everything from the start position once. Every record of a poll carries the poll's
 * timestamp T. After a restore, a poll that had already emitted rows is re-run with the same T: an
 * incremental poll from the checkpointed position, a full refresh from the start.
 *
 * <p>With a lookback, every request starts that far before the position, and the position stays at
 * the highest one emitted, so re-read records don't move it back or count as progress.
 *
 * <p>The fetcher runs ahead of the emitter, so it keeps its own position. The checkpointed position
 * is the emitter's.
 */
final class CursorSplitReader<OUT, POS, IN, R>
    implements SplitReader<CursorRecord<R, POS>, CursorSplit<POS>> {

  private static final Logger LOG = LoggerFactory.getLogger(CursorSplitReader.class);

  private static final int MAX_LOGGED_RECORD = 500;

  private final CursorSettings<OUT, POS, IN, R> settings;
  private final CursorSource<OUT, POS, IN> source;
  private final CamelRuntime runtime;
  private final RateLimiter rateLimiter;
  private final Counter droppedRecords;
  private final Counter failedRecords;

  private String splitId;
  private POS position;
  private PollClock clock;
  private long resumeT = PollClock.NONE;
  private long nextPollAt;

  /** The request to send next, or {@code null} between polls. */
  private Request pending;

  /** Where the latest {@code request(position)} started, to tell whether the poll advanced. */
  private POS requestedFrom;

  /**
   * The lowest position the next record may have: where the request started, then the last
   * record's.
   */
  private POS lowestAllowedPosition;

  private long unitT = PollClock.NONE;
  private boolean unitStarted;

  CursorSplitReader(
      CursorSettings<OUT, POS, IN, R> settings,
      CamelRuntime runtime,
      Counter droppedRecords,
      Counter failedRecords) {
    this.settings = settings;
    this.source = settings.source();
    this.runtime = runtime;
    this.rateLimiter = new RateLimiter(settings.poll().rateLimitPerMinute());
    this.droppedRecords = droppedRecords;
    this.failedRecords = failedRecords;
  }

  @Override
  public RecordsWithSplitIds<CursorRecord<R, POS>> fetch() throws IOException {
    if (splitId == null) {
      return new RecordsBySplits<>(Collections.emptyMap(), Collections.emptySet());
    }
    List<CursorRecord<R, POS>> records = new ArrayList<>();
    long timeoutMs = settings.poll().fetchTimeout().toMillis();
    if (pending == null) {
      long wait = nextPollAt - System.currentTimeMillis();
      if (wait > 0) {
        sleepQuietly(Math.min(wait, timeoutMs));
        return result(records, false);
      }
      startPoll(records);
    }

    rateLimiter.acquire();
    Request request = pending;
    Exchange response =
        runtime.sendWithRetries(
            request.uri(),
            exchange -> {
              exchange.getMessage().setBody(request.body());
              exchange.getMessage().getHeaders().putAll(request.headers());
            },
            timeoutMs);
    if (response == null) {
      // Failed, or backing off: the same request goes out again on a later fetch.
      return result(records, false);
    }

    long row = 0;
    for (IN raw : source.records(response)) {
      addRecord(raw, new RecordContext(unitT, response, ++row), records);
    }
    // records() may have consumed a streamed body that next() reads as well.
    MessageHelper.resetStreamCache(response.getMessage());

    Optional<Request> nextPage = source.next(response);
    if (nextPage.isPresent()) {
      pending = nextPage.get();
    } else if (!settings.fullRefresh() && !Objects.equals(position, requestedFrom)) {
      // Caught up with this request but not necessarily with the source: ask again.
      requestSince(position);
    } else {
      return result(records, completePoll(records));
    }
    return result(records, false);
  }

  private void startPoll(List<CursorRecord<R, POS>> records) throws IOException {
    if (resumeT != PollClock.NONE) {
      // Re-run the poll that was half emitted at the checkpoint, with the same timestamp.
      unitT = resumeT;
      resumeT = PollClock.NONE;
      records.add(CursorRecord.unitStart(unitT));
      unitStarted = true;
    } else {
      unitT = clock.next();
      unitStarted = false;
    }
    POS from = settings.fullRefresh() ? settings.startPositionOrInitial() : position;
    if (settings.fullRefresh()) {
      position = from;
    }
    requestSince(from);
  }

  private void requestSince(POS from) {
    requestedFrom = from;
    lowestAllowedPosition =
        settings.lookback().isZero() ? from : source.rewind(from, settings.lookback());
    pending = source.request(lowestAllowedPosition);
  }

  /** Ends the poll. Returns whether the split is finished. */
  private boolean completePoll(List<CursorRecord<R, POS>> records) {
    if (unitStarted) {
      records.add(CursorRecord.unitEnd(unitT));
    }
    pending = null;
    unitT = PollClock.NONE;
    unitStarted = false;
    nextPollAt = System.currentTimeMillis() + settings.poll().pollInterval().toMillis();
    return settings.poll().bounded();
  }

  private void addRecord(IN raw, RecordContext context, List<CursorRecord<R, POS>> records)
      throws IOException {
    OUT value;
    POS recordPosition;
    try {
      value = source.map(raw);
      if (value == null) {
        droppedRecords.inc();
        return;
      }
      recordPosition = source.position(value);
    } catch (Exception e) {
      failedRecords.inc();
      LOG.warn("Skipping a record that failed to map. Record: {}", truncated(raw), e);
      return;
    }
    if (!settings.fullRefresh() && isBefore(recordPosition, lowestAllowedPosition)) {
      // Restoring from a position that a later record undercut would lose the records
      // between them.
      throw new IOException(
          "The cursor went backwards from "
              + lowestAllowedPosition
              + " to "
              + recordPosition
              + ". Records must be returned in position order.");
    }
    lowestAllowedPosition = recordPosition;
    if (!unitStarted) {
      records.add(CursorRecord.unitStart(unitT));
      unitStarted = true;
    }
    if (!isBefore(recordPosition, position)) {
      position = recordPosition;
    }
    records.add(CursorRecord.record(convert(value, context), position, unitT));
  }

  /** The converted record, or {@code null} when the converter fails: its position still counts. */
  private R convert(OUT value, RecordContext context) {
    try {
      return settings.converter().convert(value, context);
    } catch (Exception e) {
      failedRecords.inc();
      LOG.warn("Skipping a record that failed to convert. Record: {}", truncated(value), e);
      return null;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean isBefore(Object a, Object b) {
    return a instanceof Comparable comparable
        && b != null
        && a.getClass() == b.getClass()
        && comparable.compareTo(b) < 0;
  }

  private RecordsWithSplitIds<CursorRecord<R, POS>> result(
      List<CursorRecord<R, POS>> records, boolean finished) {
    String id = splitId;
    if (finished) {
      LOG.info("Completed the single poll of a bounded cursor source, finishing");
      splitId = null;
    }
    return new RecordsBySplits<>(
        records.isEmpty() ? Map.of() : Map.of(id, records), finished ? Set.of(id) : Set.of());
  }

  private static String truncated(Object raw) {
    String text = String.valueOf(raw);
    return text.length() <= MAX_LOGGED_RECORD ? text : text.substring(0, MAX_LOGGED_RECORD) + "...";
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void handleSplitsChanges(SplitsChange<CursorSplit<POS>> splitsChanges) {
    if (!(splitsChanges instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
          "Unsupported splits change " + splitsChanges.getClass());
    }
    for (CursorSplit<POS> split : splitsChanges.splits()) {
      splitId = split.splitId();
      try {
        position = split.position() != null ? split.position() : settings.startPositionOrInitial();
      } catch (IOException e) {
        throw new IllegalStateException("Cannot read the configured start position", e);
      }
      clock = new PollClock(split.lastT());
      resumeT = split.unitT();
      clock.observe(resumeT);
      LOG.info(
          "Reading in cursor mode from position {}{}",
          position,
          resumeT != PollClock.NONE ? ", resuming the poll at " + resumeT : "");
    }
  }

  @Override
  public void wakeUp() {
    // fetch() blocks for at most the fetch timeout, plus one Camel request.
  }

  @Override
  public void close() {
    // The CamelContext belongs to the reader, which outlives this fetcher.
  }
}
