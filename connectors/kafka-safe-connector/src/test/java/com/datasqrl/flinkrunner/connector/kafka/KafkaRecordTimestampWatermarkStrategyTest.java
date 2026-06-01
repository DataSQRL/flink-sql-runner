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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

/** Tests for {@link KafkaRecordTimestampWatermarkStrategy}. */
class KafkaRecordTimestampWatermarkStrategyTest {

  @Test
  void testDoesNotEmitBeforeWarmup() {
    final WatermarkGenerator<RowData> generator = createGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < KafkaRecordTimestampWatermarkStrategy.MIN_RECORDS - 1; i++) {
      generator.onEvent(null, i, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).isEmpty();
  }

  @Test
  void testEmitsWatermarkFromKafkaRecordTimestampAfterWarmup() {
    final WatermarkGenerator<RowData> generator = createGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < KafkaRecordTimestampWatermarkStrategy.MIN_RECORDS; i++) {
      generator.onEvent(null, i * 100L, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks)
        .containsExactly(
            (KafkaRecordTimestampWatermarkStrategy.MIN_RECORDS - 1) * 100L
                - KafkaRecordTimestampWatermarkStrategy.MIN_OUT_OF_ORDERNESS_MILLIS
                - 1);
  }

  @Test
  void testWatermarksAreMonotonic() {
    final WatermarkGenerator<RowData> generator = createGenerator();
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < KafkaRecordTimestampWatermarkStrategy.MIN_RECORDS; i++) {
      generator.onEvent(null, i * 100L, output);
    }
    generator.onPeriodicEmit(output);

    for (int i = 0; i < KafkaRecordTimestampWatermarkStrategy.MIN_RECORDS; i++) {
      generator.onEvent(null, 0L, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).hasSize(1);
  }

  private static WatermarkGenerator<RowData> createGenerator() {
    return KafkaRecordTimestampWatermarkStrategy.INSTANCE.createWatermarkGenerator(null);
  }

  private static final class CollectingWatermarkOutput implements WatermarkOutput {

    private final List<Long> watermarks = new ArrayList<>();

    @Override
    public void emitWatermark(Watermark watermark) {
      watermarks.add(watermark.getTimestamp());
    }

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }
}
