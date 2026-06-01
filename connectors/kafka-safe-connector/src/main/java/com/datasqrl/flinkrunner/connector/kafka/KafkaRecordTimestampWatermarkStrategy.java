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

import java.io.Serial;
import java.time.Duration;
import java.util.Arrays;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;

/** Adaptive watermark strategy based on Kafka record timestamps. */
@Internal
public final class KafkaRecordTimestampWatermarkStrategy implements WatermarkStrategy<RowData> {

  @Serial private static final long serialVersionUID = 1L;

  static final int MIN_RECORDS = 250;
  static final int SAMPLE_SIZE = 4096;
  static final long MIN_OUT_OF_ORDERNESS_MILLIS = 50L;
  static final long MAX_OUT_OF_ORDERNESS_MILLIS = Duration.ofDays(1).toMillis();

  private static final double OUT_OF_ORDERNESS_QUANTILE = 0.95D;

  public static final KafkaRecordTimestampWatermarkStrategy INSTANCE =
      new KafkaRecordTimestampWatermarkStrategy();

  private KafkaRecordTimestampWatermarkStrategy() {}

  @Override
  public WatermarkGenerator<RowData> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {

    return new AdaptiveKafkaRecordTimestampWatermarkGenerator();
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

    /** Highest Kafka record timestamp observed by this generator. */
    private long maxTimestamp = Long.MIN_VALUE;

    /** Last emitted watermark, used to preserve Flink's monotonic watermark contract. */
    private long lastEmittedWatermark = Long.MIN_VALUE;

    /**
     * Number of records observed, including the first record that cannot produce a lateness sample.
     */
    private long recordCount;

    /** Next slot in the circular lateness sample buffer. */
    private int nextSampleIndex;

    @Override
    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
      if (maxTimestamp != Long.MIN_VALUE) {
        // Sample lateness before updating maxTimestamp, so the sample reflects disorder relative to
        // the already observed stream frontier.
        latenessSamples[nextSampleIndex] = Math.max(0L, maxTimestamp - eventTimestamp);
        nextSampleIndex = (nextSampleIndex + 1) % SAMPLE_SIZE;
      }

      maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
      recordCount++;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
      if (recordCount < MIN_RECORDS || maxTimestamp == Long.MIN_VALUE) {
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

    private long calculateOutOfOrdernessMillis() {
      int sampleCount = (int) Math.min(recordCount - 1, SAMPLE_SIZE);
      if (sampleCount <= 0) {
        return MIN_OUT_OF_ORDERNESS_MILLIS;
      }

      long[] samples = Arrays.copyOf(latenessSamples, sampleCount);
      Arrays.sort(samples);

      // Use a high quantile rather than the maximum, so a single pathological record does not stall
      // all event-time progress until it rotates out of the sample buffer.
      int quantileIndex =
          Math.min(sampleCount - 1, (int) Math.ceil(sampleCount * OUT_OF_ORDERNESS_QUANTILE) - 1);

      return Math.min(
          MAX_OUT_OF_ORDERNESS_MILLIS,
          Math.max(MIN_OUT_OF_ORDERNESS_MILLIS, samples[quantileIndex]));
    }
  }
}
