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
package com.datasqrl.flinkrunner.connector.datagen.metronome;

import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplit;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

/** Source reader that emits due metronome rows and snapshots sequence progress in its split. */
public class MetronomeReader implements SourceReader<RowData, MetronomeSplit> {

  public static final long UNINITIALIZED = Long.MIN_VALUE;

  private final SourceReaderContext readerContext;
  @Nullable private final Long numberOfRows;
  private final ScheduledExecutorService availabilityExecutor;

  private CompletableFuture<Void> availability;
  private boolean noMoreSplits;
  private boolean closed;
  private boolean splitAssigned;
  private long lastEmittedNumber;
  private long lastEmissionTimestampMillis;

  public MetronomeReader(SourceReaderContext readerContext, @Nullable Long numberOfRows) {
    this.readerContext = readerContext;
    this.numberOfRows = numberOfRows;
    this.availability = new CompletableFuture<>();
    this.availabilityExecutor =
        Executors.newSingleThreadScheduledExecutor(new ExecutorThreadFactory("metronome-source"));
    this.lastEmittedNumber = 0L;
    this.lastEmissionTimestampMillis = UNINITIALIZED;
  }

  @Override
  public void start() {
    readerContext.sendSplitRequest();
  }

  /**
   * Emits the next due metronome row, or schedules the reader to become available at the next
   * second boundary.
   *
   * <p>The emitted sequence number is always the next number after the checkpointed progress. The
   * reader never drains wall-clock backlog in a burst: after every emitted row, it records the
   * current time so the next row cannot become due before a full second has elapsed.
   */
  @Override
  public InputStatus pollNext(ReaderOutput<RowData> output) {
    if (closed) {
      return InputStatus.END_OF_INPUT;
    }

    if (!splitAssigned) {
      return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
    }

    var currentTimestamp = Instant.now();
    long currentTimestampMillis = currentTimestamp.toEpochMilli();

    if (isDue(currentTimestampMillis)
        && (numberOfRows == null || lastEmittedNumber < numberOfRows)) {
      long nextNumber = lastEmittedNumber + 1;
      var row = GenericRowData.of(nextNumber, TimestampData.fromInstant(currentTimestamp));
      lastEmittedNumber = nextNumber;
      lastEmissionTimestampMillis = currentTimestampMillis;
      output.collect(row, currentTimestampMillis);
    }

    return nextStatus();
  }

  /**
   * Snapshots the assigned split as the reader's progress state.
   *
   * <p>The split carries the only source progress that must survive failover: the last emitted
   * sequence number.
   */
  @Override
  public List<MetronomeSplit> snapshotState(long checkpointId) {
    if (!splitAssigned || closed || (numberOfRows != null && lastEmittedNumber >= numberOfRows)) {
      return List.of();
    }

    return List.of(new MetronomeSplit(lastEmittedNumber));
  }

  /** Returns the current availability future used by the Flink runtime to avoid busy polling. */
  @Override
  public CompletableFuture<Void> isAvailable() {
    return availability;
  }

  /** Restores progress from the assigned split and makes the reader immediately pollable. */
  @Override
  public void addSplits(List<MetronomeSplit> splits) {
    for (var split : splits) {
      lastEmittedNumber = split.lastEmittedNumber();
      splitAssigned = true;
      break;
    }
    completeAvailability();
  }

  @Override
  public void notifyNoMoreSplits() {
    noMoreSplits = true;
    completeAvailability();
  }

  /** Closes the reader and wakes any runtime caller blocked on the current availability future. */
  @Override
  public void close() {
    closed = true;
    availabilityExecutor.shutdownNow();
    completeAvailability();
  }

  /**
   * Determines the next source status after a poll cycle.
   *
   * <p>The source ends when bounded output is exhausted or the sequence reaches {@link
   * Long#MAX_VALUE}. Otherwise, it replaces the availability future and schedules it one second
   * later.
   */
  private InputStatus nextStatus() {
    if (lastEmittedNumber == Long.MAX_VALUE
        || (numberOfRows != null && lastEmittedNumber >= numberOfRows)) {
      closed = true;
      return InputStatus.END_OF_INPUT;
    }

    availability = new CompletableFuture<>();
    scheduleAvailability();

    return InputStatus.NOTHING_AVAILABLE;
  }

  private boolean isDue(long currentTimestampMillis) {
    return lastEmissionTimestampMillis == UNINITIALIZED
        || currentTimestampMillis - lastEmissionTimestampMillis >= TimeUnit.SECONDS.toMillis(1);
  }

  private void completeAvailability() {
    availability.complete(null);
  }

  /** Schedules completion of the current availability future one second later. */
  private void scheduleAvailability() {
    if (!closed) {
      // Capture the current future so stale timers cannot wake newer poll cycles
      var scheduledAvailability = availability;
      availabilityExecutor.schedule(
          () -> scheduledAvailability.complete(null), 1, TimeUnit.SECONDS);
    }
  }
}
