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

import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT;
import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.MIN_RECORDS_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;
import org.junit.jupiter.api.Test;

/** Tests for {@link KafkaRecordTimestampWatermarkStrategy}. */
class KafkaRecordTimestampWatermarkStrategyTest {

  @Test
  void testDoesNotEmitBeforeWarmup() {
    final WatermarkGenerator<RowData> generator = createOnPeriodicGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT - 1; i++) {
      generator.onEvent(null, i, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).isEmpty();
  }

  @Test
  void testEmitsWatermarkFromKafkaRecordTimestampAfterWarmup() {
    final WatermarkGenerator<RowData> generator = createOnPeriodicGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, i * 100L, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks)
        .containsExactly(
            (MIN_RECORDS_DEFAULT - 1) * 100L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1);
  }

  @Test
  void testOnEventEmitStrategyEmitsWhenWarmupCompletes() {
    final WatermarkGenerator<RowData> generator = createOnEventGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, i * 100L, output);
    }

    assertThat(output.watermarks)
        .containsExactly(
            (MIN_RECORDS_DEFAULT - 1) * 100L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1);
  }

  @Test
  void testUsesConfiguredWarmupAndMinimumOutOfOrderness() {
    final WatermarkGenerator<RowData> generator =
        createOnPeriodicGenerator(
            new SourceWatermarkConfig(3, 10, 1000, 0.95D, 0, 10_000, 1000, 5000));
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    generator.onEvent(null, 0L, output);
    generator.onEvent(null, 100L, output);
    generator.onEvent(null, 200L, output);
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).containsExactly(189L);
  }

  @Test
  void testWatermarksAreMonotonic() {
    final WatermarkGenerator<RowData> generator = createOnPeriodicGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, i * 100L, output);
    }
    generator.onPeriodicEmit(output);

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, 0L, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).hasSize(1);
  }

  @Test
  void testIdleAdvanceEmitsAfterSilenceBeforeWarmupCompletes() {
    final MutableMillisClock clock = new MutableMillisClock();
    final WatermarkGenerator<RowData> generator =
        new KafkaRecordTimestampWatermarkStrategy(
                WatermarkEmitStrategy.ON_PERIODIC,
                new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 5000),
                clock,
                currentTimeMillis -> true)
            .createWatermarkGenerator(null);
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    generator.onEvent(null, 10_000L, output);
    generator.onPeriodicEmit(output);

    clock.advanceMillis(999);
    generator.onPeriodicEmit(output);

    clock.advanceMillis(1);
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).containsExactly(949L);

    clock.advanceMillis(10_000);
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).containsExactly(949L, 10_949L);
  }

  @Test
  void testIdleAdvanceDoesNotEmitWhenReadinessCheckFails() {
    final MutableMillisClock clock = new MutableMillisClock();
    final WatermarkGenerator<RowData> generator =
        new KafkaRecordTimestampWatermarkStrategy(
                WatermarkEmitStrategy.ON_PERIODIC,
                new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 5000),
                clock,
                currentTimeMillis -> false)
            .createWatermarkGenerator(null);
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    generator.onEvent(null, 10_000L, output);
    clock.advanceMillis(11_000);
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).isEmpty();
  }

  @Test
  void testIdleAdvanceMarksNeverSeenOutputIdleAndReactivatesOnFirstRecord() {
    final MutableMillisClock clock = new MutableMillisClock();
    final WatermarkGenerator<RowData> generator =
        new KafkaRecordTimestampWatermarkStrategy(
                WatermarkEmitStrategy.ON_PERIODIC,
                new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 5000),
                clock,
                currentTimeMillis -> true)
            .createWatermarkGenerator(null);
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    clock.advanceMillis(999);
    generator.onPeriodicEmit(output);

    assertThat(output.idleCount).isZero();

    clock.advanceMillis(1);
    generator.onPeriodicEmit(output);

    assertThat(output.idleCount).isOne();
    assertThat(output.activeCount).isZero();
    assertThat(output.watermarks).isEmpty();

    generator.onPeriodicEmit(output);

    assertThat(output.idleCount).isOne();

    generator.onEvent(null, 10_000L, output);

    assertThat(output.activeCount).isOne();
  }

  private static WatermarkGenerator<RowData> createOnPeriodicGenerator() {
    return new KafkaRecordTimestampWatermarkStrategy(
            WatermarkEmitStrategy.ON_PERIODIC, new SourceWatermarkConfig())
        .createWatermarkGenerator(null);
  }

  private static WatermarkGenerator<RowData> createOnPeriodicGenerator(
      SourceWatermarkConfig configuration) {
    return new KafkaRecordTimestampWatermarkStrategy(
            WatermarkEmitStrategy.ON_PERIODIC, configuration)
        .createWatermarkGenerator(null);
  }

  private static WatermarkGenerator<RowData> createOnEventGenerator() {
    return new KafkaRecordTimestampWatermarkStrategy(
            WatermarkEmitStrategy.ON_EVENT, new SourceWatermarkConfig())
        .createWatermarkGenerator(null);
  }

  private static final class CollectingWatermarkOutput implements WatermarkOutput {

    private final List<Long> watermarks = new ArrayList<>();
    private int idleCount;
    private int activeCount;

    @Override
    public void emitWatermark(Watermark watermark) {
      watermarks.add(watermark.getTimestamp());
    }

    @Override
    public void markIdle() {
      idleCount++;
    }

    @Override
    public void markActive() {
      activeCount++;
    }
  }

  private static final class MutableMillisClock
      implements KafkaRecordTimestampWatermarkStrategy.MillisClock {

    private long currentTimeMillis;

    @Override
    public long currentTimeMillis() {
      return currentTimeMillis;
    }

    private void advanceMillis(long millis) {
      currentTimeMillis += millis;
    }
  }
}
