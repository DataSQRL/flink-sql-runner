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
import java.io.Serializable;
import java.time.Duration;
import lombok.Value;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

public class SourceWatermarkOptions {

  public static final ConfigOption<Integer> SCAN_SOURCE_WATERMARK_MIN_RECORDS =
      ConfigOptions.key("scan.source-watermark.min-records")
          .intType()
          .defaultValue(KafkaRecordTimestampWatermarkStrategy.MIN_RECORDS)
          .withDescription(
              "Minimum number of records to observe before emitting source watermarks.");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS =
      ConfigOptions.key("scan.source-watermark.min-out-of-orderness")
          .durationType()
          .defaultValue(
              Duration.ofMillis(KafkaRecordTimestampWatermarkStrategy.MIN_OUT_OF_ORDERNESS_MILLIS))
          .withDescription("Minimum adaptive out-of-orderness delay for source watermarks.");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_MAX_OUT_OF_ORDERNESS =
      ConfigOptions.key("scan.source-watermark.max-out-of-orderness")
          .durationType()
          .defaultValue(
              Duration.ofMillis(KafkaRecordTimestampWatermarkStrategy.MAX_OUT_OF_ORDERNESS_MILLIS))
          .withDescription("Maximum adaptive out-of-orderness delay for source watermarks.");

  public static final ConfigOption<Double> SCAN_SOURCE_WATERMARK_OUT_OF_ORDERNESS_QUANTILE =
      ConfigOptions.key("scan.source-watermark.out-of-orderness-quantile")
          .doubleType()
          .defaultValue(KafkaRecordTimestampWatermarkStrategy.OUT_OF_ORDERNESS_QUANTILE)
          .withDescription(
              "Quantile of observed lateness samples to use as the source watermark delay.");

  public static SourceWatermarkConfig sourceWatermarkConfiguration(ReadableConfig tableOptions) {
    var minRecords = tableOptions.get(SCAN_SOURCE_WATERMARK_MIN_RECORDS);
    var minOutOfOrderness = tableOptions.get(SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS);
    var maxOutOfOrderness = tableOptions.get(SCAN_SOURCE_WATERMARK_MAX_OUT_OF_ORDERNESS);
    var quantile = tableOptions.get(SCAN_SOURCE_WATERMARK_OUT_OF_ORDERNESS_QUANTILE);

    if (minRecords <= 0) {
      throw new ValidationException(
          String.format("'%s' must be greater than 0.", SCAN_SOURCE_WATERMARK_MIN_RECORDS.key()));
    }

    if (minOutOfOrderness.isNegative()) {
      throw new ValidationException(
          String.format(
              "'%s' must not be negative.", SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS.key()));
    }

    if (maxOutOfOrderness.compareTo(minOutOfOrderness) < 0) {
      throw new ValidationException(
          String.format(
              "'%s' must be greater than or equal to '%s'.",
              SCAN_SOURCE_WATERMARK_MAX_OUT_OF_ORDERNESS.key(),
              SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS.key()));
    }

    if (quantile <= 0D || quantile > 1D) {
      throw new ValidationException(
          String.format(
              "'%s' must be greater than 0 and less than or equal to 1.",
              SCAN_SOURCE_WATERMARK_OUT_OF_ORDERNESS_QUANTILE.key()));
    }

    return new SourceWatermarkConfig(
        minRecords, minOutOfOrderness.toMillis(), maxOutOfOrderness.toMillis(), quantile);
  }

  @Value
  public static class SourceWatermarkConfig implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    int minRecords;
    long minOutOfOrdernessMillis;
    long maxOutOfOrdernessMillis;
    double outOfOrdernessQuantile;
  }
}
