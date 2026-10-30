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
package com.datasqrl.flinkrunner.connector.kafka;

import static org.apache.flink.util.Preconditions.checkArgument;

import com.datasqrl.flinkrunner.connector.kafka.KafkaAdminIdleAdvanceReadinessChecker.LogEnd;
import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;
import org.apache.flink.util.clock.RelativeClock;

/** Adaptive watermark strategy based on Kafka record timestamps. */
@Internal
public final class KafkaRecordTimestampWatermarkStrategy implements WatermarkStrategy<RowData> {

  @Serial private static final long serialVersionUID = 1L;

  private static final int SAMPLE_SIZE = 4096;

  private final WatermarkEmitStrategy emitStrategy;
  private final SourceWatermarkConfig sourceWatermarkConfig;
  private final MillisClock clock;
  @Nullable private final KafkaAdminIdleAdvanceReadinessChecker idleAdvanceReadinessChecker;

  /** Takes an idle advance readiness checker if and only if idle advancement is enabled. */
  public KafkaRecordTimestampWatermarkStrategy(
      WatermarkEmitStrategy emitStrategy,
      SourceWatermarkConfig sourceWatermarkConfig,
      @Nullable KafkaAdminIdleAdvanceReadinessChecker idleAdvanceReadinessChecker) {
    this(
        emitStrategy,
        sourceWatermarkConfig,
        System::currentTimeMillis,
        idleAdvanceReadinessChecker);
  }

  @VisibleForTesting
  KafkaRecordTimestampWatermarkStrategy(
      WatermarkEmitStrategy emitStrategy,
      SourceWatermarkConfig sourceWatermarkConfig,
      MillisClock clock,
      @Nullable KafkaAdminIdleAdvanceReadinessChecker idleAdvanceReadinessChecker) {
    checkArgument(
        sourceWatermarkConfig.idleAdvanceTimeoutMillis() <= 0
            || idleAdvanceReadinessChecker != null,
        "Idle watermark advancement needs a readiness checker.");
    this.emitStrategy = emitStrategy;
    this.sourceWatermarkConfig = sourceWatermarkConfig;
    this.clock = clock;
    this.idleAdvanceReadinessChecker = idleAdvanceReadinessChecker;
  }

  @Override
  public WatermarkGenerator<RowData> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {

    return new AdaptiveKafkaRecordTimestampWatermarkGenerator(
        emitStrategy,
        sourceWatermarkConfig,
        clock,
        context.getInputActivityClock(),
        idleAdvanceReadinessChecker);
  }

  /**
   * Watermark generator that derives event time from Kafka record timestamps and adapts its
   * out-of-orderness delay from recently observed records.
   *
   * <p>The generator watches how far records tend to arrive behind the newest Kafka timestamp seen
   * so far. It keeps a rolling sample of those delays and uses the 95th percentile as the safety
   * margin before advancing the watermark. This lets the source tolerate typical out-of-order
   * records without waiting for rare extreme delays.
   *
   * <p>The {@code eventTimestamp} argument is supplied by Flink from the Kafka source record
   * timestamp.
   *
   * <p>With idle advancement enabled, a split is idle once it has read nothing for the idle-advance
   * timeout. An idle split advances on wall-clock time, but only as far as a log-end fetch that
   * shows it had read everything: a split can also be idle with records still unread, when the
   * source is busy with other partitions or rate-limited. Idle time is measured on Flink's input
   * activity clock for the split, which stops while the split is paused for watermark alignment or
   * the task is backpressured, and against the read position {@link KafkaReadPositions} tracks,
   * which also moves for records that produce no row. Offsets that hold no record, such as
   * transaction markers, count as read once the consumer has moved past them ({@link
   * KafkaFetchPositions}) and the split has read every record before them.
   */
  private static final class AdaptiveKafkaRecordTimestampWatermarkGenerator
      implements WatermarkGenerator<RowData> {

    /**
     * Circular buffer of observed lateness values in milliseconds.
     *
     * <p>For a record with timestamp {@code t}, lateness is {@code max(0, maxTimestamp - t)} using
     * the maximum timestamp seen before that record. In-order records therefore contribute {@code
     * 0}, while older records contribute how far they lag behind the current observed frontier.
     */
    private final long[] latenessSamples = new long[SAMPLE_SIZE];

    private final WatermarkEmitStrategy emitStrategy;
    private final SourceWatermarkConfig config;
    private final MillisClock clock;

    /** Present whenever idle advancement is enabled. */
    @Nullable private final KafkaAdminIdleAdvanceReadinessChecker idleAdvanceReadinessChecker;

    /** Measures idle time; stops while the split is paused or backpressured. */
    private final RelativeClock inputActivityClock;

    /** Highest Kafka record timestamp observed by this generator. */
    private long maxTimestamp = Long.MIN_VALUE;

    /** Processing time when the latest record was observed. */
    private long lastRecordWallClockMillis = Long.MIN_VALUE;

    /** How far this split's partition has been read, known from the split's first row on. */
    @Nullable private ReadPosition readPosition;

    /** Read progress at the previous idle check, to tell whether anything was consumed since. */
    private long progressAtLastIdleCheck;

    /**
     * Input activity time since which the split has been idle: creation, or the first idle check
     * that saw its latest progress.
     */
    private long idleSinceInputActivityMillis;

    /** Last emitted watermark, used to preserve Flink's monotonic watermark contract. */
    private long lastEmittedWatermark = Long.MIN_VALUE;

    /** Whether this output was marked idle before receiving any records. */
    private boolean markedIdle;

    /**
     * Number of records observed, including the first record that cannot produce a lateness sample.
     */
    private long recordCount;

    /** Next slot in the circular lateness sample buffer. */
    private int nextSampleIndex;

    private AdaptiveKafkaRecordTimestampWatermarkGenerator(
        WatermarkEmitStrategy emitStrategy,
        SourceWatermarkConfig config,
        MillisClock clock,
        RelativeClock inputActivityClock,
        @Nullable KafkaAdminIdleAdvanceReadinessChecker idleAdvanceReadinessChecker) {
      this.emitStrategy = emitStrategy;
      this.config = config;
      this.clock = clock;
      this.inputActivityClock = inputActivityClock;
      this.idleAdvanceReadinessChecker = idleAdvanceReadinessChecker;
      this.idleSinceInputActivityMillis = inputActivityClock.relativeTimeMillis();
    }

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
      boolean wasMarkedIdle = markedIdle;
      if (markedIdle) {
        output.markActive();
        markedIdle = false;
      }

      if (readPosition == null && config.idleAdvanceTimeoutMillis() > 0) {
        // The record that produced this row is the one the thread consumed last.
        readPosition = KafkaReadPositions.latest();
        if (readPosition != null) {
          idleAdvanceReadinessChecker.register(readPosition.partition());
        }
      }

      if (maxTimestamp != Long.MIN_VALUE) {
        // Sample lateness before updating maxTimestamp, so the sample reflects disorder relative to
        // the already observed stream frontier.
        latenessSamples[nextSampleIndex] = Math.max(0L, maxTimestamp - eventTimestamp);
        nextSampleIndex = (nextSampleIndex + 1) % SAMPLE_SIZE;
      }

      maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
      lastRecordWallClockMillis = clock.currentTimeMillis();
      recordCount++;

      if (recordCount == 1
          && config.idleAdvanceTimeoutMillis() > 0
          && (wasMarkedIdle || idleAdvanceReadinessChecker.isReady(lastRecordWallClockMillis))) {
        // Flink 2.3.0 reports this source subtask active again only along with a watermark that
        // moves its watermark forward (FLINK-40504), and reports it idle while none of its splits
        // has emitted one. Downstream operators would then learn that it is active from the same
        // watermark that releases this row, and a parallel operator could forward that watermark
        // ahead of the row. A watermark just below the row makes the subtask active first. It
        // cannot pass a later row of the partition: the broker timestamps them as they arrive.
        emitIfHigher(output, eventTimestamp - calculateOutOfOrdernessMillis() - 1);
      }

      if (emitStrategy.isOnEvent()) {
        emitIfReady(output);
      }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      if (emitStrategy.isOnPeriodic()) {
        emitIfReady(output);
      }

      emitIdleWatermarkIfReady(output);
    }

    private void emitIfReady(WatermarkOutput output) {
      if (recordCount < config.minRecords() || maxTimestamp == Long.MIN_VALUE) {
        // Avoid emitting during warm-up, so limited early samples won't distort the estimate.
        return;
      }

      emitIfHigher(output, maxTimestamp - calculateOutOfOrdernessMillis() - 1);
    }

    private void emitIdleWatermarkIfReady(WatermarkOutput output) {
      long idleAdvanceTimeoutMillis = config.idleAdvanceTimeoutMillis();

      if (idleAdvanceTimeoutMillis <= 0) {
        return;
      }

      // Read the clock here rather than per record, like Flink's own idleness: the split then
      // counts as idle from the first check after its latest progress, up to one interval late,
      // never early.
      long inputActivityMillis = inputActivityClock.relativeTimeMillis();
      long progress = readPosition != null ? readPosition.nextOffset() : recordCount;
      if (progress != progressAtLastIdleCheck) {
        progressAtLastIdleCheck = progress;
        idleSinceInputActivityMillis = inputActivityMillis;
      }

      if (inputActivityMillis - idleSinceInputActivityMillis < idleAdvanceTimeoutMillis) {
        return;
      }

      long currentTimeMillis = clock.currentTimeMillis();
      if (recordCount == 0) {
        if (!markedIdle
            && idleAdvanceReadinessChecker.unreadAssignmentsReachedLogEnd(currentTimeMillis)) {
          output.markIdle();
          markedIdle = true;
        }
        return;
      }

      LogEnd logEnd =
          readPosition == null
              ? null
              : idleAdvanceReadinessChecker.logEndReachedBy(readPosition, currentTimeMillis);

      if (logEnd == null) {
        // Records may still be unread, so the watermark stays where the records put it.
        return;
      }

      long outOfOrdernessMillis = calculateOutOfOrdernessMillis();

      // Every record appended before the fetch was issued has been read. When the latest record
      // was read, broker time had passed its timestamp and, up to clock skew, the read time
      // itself: for a record read out of a backlog the read time is later. Start from the later
      // of the two and move to the time of the fetch, not to now, since records appended after it
      // may still be unread. A fetch older than the latest read therefore moves the watermark
      // less, never too far. Then subtract the observed lateness estimate and the safety margin,
      // which also absorbs the clock skew, to avoid closing windows too early.
      long frontierMillis =
          Math.max(maxTimestamp, lastRecordWallClockMillis)
              + (logEnd.requestedAtMillis() - lastRecordWallClockMillis);
      emitIfHigher(
          output,
          frontierMillis - outOfOrdernessMillis - config.idleAdvanceSafetyMarginMillis() - 1);
    }

    /** Emits {@code watermark} if it moves the watermark forward, as Flink requires. */
    private void emitIfHigher(WatermarkOutput output, long watermark) {
      if (watermark > lastEmittedWatermark) {
        output.emitWatermark(new Watermark(watermark));
        lastEmittedWatermark = watermark;
      }
    }

    private long calculateOutOfOrdernessMillis() {
      int sampleCount = (int) Math.min(recordCount - 1, SAMPLE_SIZE);
      if (sampleCount <= 0) {
        return config.minOutOfOrdernessMillis();
      }

      long[] samples = Arrays.copyOf(latenessSamples, sampleCount);
      Arrays.sort(samples);

      // Use a high quantile rather than the maximum, so a single pathological record does not stall
      // all event-time progress until it rotates out of the sample buffer.
      int quantileIndex =
          Math.min(
              sampleCount - 1, (int) Math.ceil(sampleCount * config.outOfOrdernessQuantile()) - 1);

      return Math.min(
          config.maxOutOfOrdernessMillis(),
          Math.max(config.minOutOfOrdernessMillis(), samples[quantileIndex]));
    }
  }

  @FunctionalInterface
  interface MillisClock extends Serializable {

    long currentTimeMillis();
  }
}
