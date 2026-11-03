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

import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.sourceWatermarkParams;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;
import org.apache.flink.table.watermark.WatermarkParams;
import org.junit.jupiter.api.Test;

class SourceWatermarkOptionsTest {

  @Test
  void givenWatermarkOptions_whenReadingParams_thenCarriesThem() {
    var options =
        Map.of(
            "scan.watermark.emit.strategy", "on-event",
            "scan.watermark.alignment.group", "group",
            "scan.watermark.alignment.max-drift", "2 min",
            "scan.watermark.alignment.update-interval", "200 ms",
            "scan.watermark.idle-timeout", "30 s");

    assertThat(sourceWatermarkParams(Configuration.fromMap(options)))
        .isEqualTo(
            new WatermarkParams(
                WatermarkEmitStrategy.ON_EVENT,
                "group",
                Duration.ofMinutes(2),
                Duration.ofMillis(200),
                30_000L));
  }

  @Test
  void givenNoWatermarkOptions_whenReadingParams_thenUsesFlinksDefaults() {
    var params = sourceWatermarkParams(new Configuration());

    assertThat(params).isEqualTo(WatermarkParams.builder().build());
    assertThat(params.alignWatermarkEnabled()).isFalse();
    assertThat(params.getSourceIdleTimeout()).isNegative();
  }
}
