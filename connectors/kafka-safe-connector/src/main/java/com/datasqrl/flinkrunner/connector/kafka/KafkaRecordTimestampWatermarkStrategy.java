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

import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.io.Serial;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;

/** Adaptive watermark strategy based on Kafka record timestamps. */
@Internal
public final class KafkaRecordTimestampWatermarkStrategy implements WatermarkStrategy<RowData> {

  @Serial private static final long serialVersionUID = 1L;

  private static final int SAMPLE_SIZE = 4096;

  private final WatermarkEmitStrategy emitStrategy;
  private final SourceWatermarkConfig sourceWatermarkConfig;
  private final MillisClock clock;
  private final IdleAdvanceReadinessChecker idleAdvanceReadinessChecker;

  public KafkaRecordTimestampWatermarkStrategy(
      WatermarkEmitStrategy emitStrategy,
      SourceWatermarkConfig sourceWatermarkConfig,
      KafkaAdminIdleAdvanceReadinessChecker idleAdvanceReadinessChecker) {
    this(
        emitStrategy,
        sourceWatermarkConfig,
        System::currentTimeMillis,
        idleAdvanceReadinessChecker);
  }

  public KafkaRecordTimestampWatermarkStrategy(
      WatermarkEmitStrategy emitStrategy, SourceWatermarkConfig sourceWatermarkConfig) {
    this(
        emitStrategy, sourceWatermarkConfig, System::currentTimeMillis, currentTimeMillis -> false);
  }

  @VisibleForTesting
  KafkaRecordTimestampWatermarkStrategy(
      WatermarkEmitStrategy emitStrategy,
      SourceWatermarkConfig sourceWatermarkConfig,
      MillisClock clock,
      IdleAdvanceReadinessChecker idleAdvanceReadinessChecker) {
    this.emitStrategy = emitStrategy;
    this.sourceWatermarkConfig = sourceWatermarkConfig;
    this.clock = clock;
    this.idleAdvanceReadinessChecker = idleAdvanceReadinessChecker;
  }

  @Override
  public WatermarkGenerator<RowData> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {

    return new AdaptiveKafkaRecordTimestampWatermarkGenerator(
        emitStrategy, sourceWatermarkConfig, clock, idleAdvanceReadinessChecker);
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
    private final IdleAdvanceReadinessChecker idleAdvanceReadinessChecker;
    private final long createdWallClockMillis;

    /** Highest Kafka record timestamp observed by this generator. */
    private long maxTimestamp = Long.MIN_VALUE;

    /** Processing time when the latest record was observed. */
    private long lastRecordWallClockMillis = Long.MIN_VALUE;

    /** Last emitted watermark, used to preserve Flink's monotonic watermark contract. */
    private long lastEmittedWatermark = Long.MIN_VALUE;

    /** Whether this output was marked idle before receiving any records. */
    private boolean idle;

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
        IdleAdvanceReadinessChecker idleAdvanceReadinessChecker) {
      this.emitStrategy = emitStrategy;
      this.config = config;
      this.clock = clock;
      this.idleAdvanceReadinessChecker = idleAdvanceReadinessChecker;
      this.createdWallClockMillis = clock.currentTimeMillis();
    }

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
      if (idle) {
        output.markActive();
        idle = false;
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

      long outOfOrdernessMillis = calculateOutOfOrdernessMillis();
      long watermark = maxTimestamp - outOfOrdernessMillis - 1;

      if (watermark > lastEmittedWatermark) {
        output.emitWatermark(new Watermark(watermark));
        lastEmittedWatermark = watermark;
      }
    }

    private void emitIdleWatermarkIfReady(WatermarkOutput output) {
      long idleAdvanceTimeoutMillis = config.idleAdvanceTimeoutMillis();

      if (idleAdvanceTimeoutMillis <= 0) {
        return;
      }

      long currentTimeMillis = clock.currentTimeMillis();
      if (recordCount == 0) {
        markIdleIfReady(output, currentTimeMillis, currentTimeMillis - createdWallClockMillis);
        return;
      }

      if (maxTimestamp == Long.MIN_VALUE || lastRecordWallClockMillis == Long.MIN_VALUE) {
        return;
      }

      long idleDurationMillis = currentTimeMillis - lastRecordWallClockMillis;
      if (idleDurationMillis < idleAdvanceTimeoutMillis) {
        return;
      }

      if (!idleAdvanceReadinessChecker.isReady(currentTimeMillis)) {
        return;
      }

      long outOfOrdernessMillis = calculateOutOfOrdernessMillis();

      // Move forward from the last Kafka timestamp by wall-clock idle time, then subtract the
      // observed lateness estimate and configured safety margin to avoid closing windows too early.
      long watermark =
          maxTimestamp
              + idleDurationMillis
              - outOfOrdernessMillis
              - config.idleAdvanceSafetyMarginMillis()
              - 1;

      if (watermark > lastEmittedWatermark) {
        output.emitWatermark(new Watermark(watermark));
        lastEmittedWatermark = watermark;
      }
    }

    private void markIdleIfReady(
        WatermarkOutput output, long currentTimeMillis, long idleDurationMillis) {

      if (idle || idleDurationMillis < config.idleAdvanceTimeoutMillis()) {
        return;
      }

      if (!idleAdvanceReadinessChecker.isReady(currentTimeMillis)) {
        return;
      }

      output.markIdle();
      idle = true;
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
