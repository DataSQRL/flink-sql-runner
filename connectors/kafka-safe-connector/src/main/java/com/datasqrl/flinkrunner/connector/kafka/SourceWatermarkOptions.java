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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

public class SourceWatermarkOptions {

  static final int MIN_RECORDS_DEFAULT = 250;
  static final long MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT = 50L;
  static final long MAX_OUT_OF_ORDERNESS_MILLIS_DEFAULT = Duration.ofDays(3).toMillis();
  static final double OUT_OF_ORDERNESS_QUANTILE_DEFAULT = 0.95D;

  public static final ConfigOption<Integer> SCAN_SOURCE_WATERMARK_MIN_RECORDS =
      ConfigOptions.key("scan.source-watermark.min-records")
          .intType()
          .defaultValue(MIN_RECORDS_DEFAULT)
          .withDescription(
              "Minimum number of records to observe before emitting source watermarks.");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS =
      ConfigOptions.key("scan.source-watermark.min-out-of-orderness")
          .durationType()
          .defaultValue(Duration.ofMillis(MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT))
          .withDescription("Minimum adaptive out-of-orderness delay for source watermarks.");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_MAX_OUT_OF_ORDERNESS =
      ConfigOptions.key("scan.source-watermark.max-out-of-orderness")
          .durationType()
          .defaultValue(Duration.ofMillis(MAX_OUT_OF_ORDERNESS_MILLIS_DEFAULT))
          .withDescription("Maximum adaptive out-of-orderness delay for source watermarks.");

  public static final ConfigOption<Double> SCAN_SOURCE_WATERMARK_OUT_OF_ORDERNESS_QUANTILE =
      ConfigOptions.key("scan.source-watermark.out-of-orderness-quantile")
          .doubleType()
          .defaultValue(OUT_OF_ORDERNESS_QUANTILE_DEFAULT)
          .withDescription(
              "Quantile of observed lateness samples to use as the source watermark delay.");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_TIMEOUT =
      ConfigOptions.key("scan.source-watermark.idle-advance-timeout")
          .durationType()
          .defaultValue(Duration.ZERO)
          .withDescription(
              """
                  How long the Kafka source must receive no records before source watermarks may \
                  continue advancing based on wall-clock time. This helps event-time windows \
                  close after traffic stops. A zero or negative duration disables idle watermark \
                  advancement.""");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_SAFETY_MARGIN =
      ConfigOptions.key("scan.source-watermark.idle-advance-safety-margin")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription(
              """
                  Additional duration subtracted from wall-clock-derived idle source watermarks to \
                  keep advancement conservative.""");

  public static final ConfigOption<Duration>
      SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_BROKER_CHECK_TIMEOUT =
          ConfigOptions.key("scan.source-watermark.idle-advance-broker-check-timeout")
              .durationType()
              .defaultValue(Duration.ofSeconds(1))
              .withDescription(
                  """
                      Timeout applied to each Kafka AdminClient broker readiness and LogAppendTime \
                      metadata check before idle source-watermark advancement.""");

  public static final ConfigOption<Duration> SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_BROKER_CHECK_TTL =
      ConfigOptions.key("scan.source-watermark.idle-advance-broker-check-ttl")
          .durationType()
          .defaultValue(Duration.ofSeconds(10))
          .withDescription(
              """
                      How long to cache Kafka AdminClient broker readiness and LogAppendTime \
                      metadata check results for idle source-watermark advancement.""");

  public static SourceWatermarkConfig sourceWatermarkConfiguration(ReadableConfig tableOptions) {
    var minRecords = tableOptions.get(SCAN_SOURCE_WATERMARK_MIN_RECORDS);
    var minOutOfOrderness = tableOptions.get(SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS);
    var maxOutOfOrderness = tableOptions.get(SCAN_SOURCE_WATERMARK_MAX_OUT_OF_ORDERNESS);
    var quantile = tableOptions.get(SCAN_SOURCE_WATERMARK_OUT_OF_ORDERNESS_QUANTILE);
    var idleAdvanceTimeout = tableOptions.get(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_TIMEOUT);
    var idleAdvanceSafetyMargin =
        tableOptions.get(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_SAFETY_MARGIN);
    var idleAdvanceBrokerCheckTimeout =
        tableOptions.get(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_BROKER_CHECK_TIMEOUT);
    var idleAdvanceBrokerCheckTtl =
        tableOptions.get(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_BROKER_CHECK_TTL);

    if (minRecords <= 0) {
      throw new ValidationException(
          "'%s' must be greater than 0.".formatted(SCAN_SOURCE_WATERMARK_MIN_RECORDS.key()));
    }

    if (minOutOfOrderness.isNegative()) {
      throw new ValidationException(
          "'%s' must not be negative.".formatted(SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS.key()));
    }

    if (maxOutOfOrderness.compareTo(minOutOfOrderness) < 0) {
      throw new ValidationException(
          "'%s' must be greater than or equal to '%s'."
              .formatted(
                  SCAN_SOURCE_WATERMARK_MAX_OUT_OF_ORDERNESS.key(),
                  SCAN_SOURCE_WATERMARK_MIN_OUT_OF_ORDERNESS.key()));
    }

    if (quantile <= 0D || quantile > 1D) {
      throw new ValidationException(
          "'%s' must be greater than 0 and less than or equal to 1."
              .formatted(SCAN_SOURCE_WATERMARK_OUT_OF_ORDERNESS_QUANTILE.key()));
    }

    if (idleAdvanceTimeout.isNegative()) {
      throw new ValidationException(
          "'%s' must not be negative.".formatted(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_TIMEOUT.key()));
    }

    if (idleAdvanceSafetyMargin.isNegative()) {
      throw new ValidationException(
          "'%s' must not be negative."
              .formatted(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_SAFETY_MARGIN.key()));
    }

    if (idleAdvanceBrokerCheckTimeout.isNegative() || idleAdvanceBrokerCheckTimeout.isZero()) {
      throw new ValidationException(
          "'%s' must be greater than 0."
              .formatted(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_BROKER_CHECK_TIMEOUT.key()));
    }

    if (idleAdvanceBrokerCheckTtl.isNegative() || idleAdvanceBrokerCheckTtl.isZero()) {
      throw new ValidationException(
          "'%s' must be greater than 0."
              .formatted(SCAN_SOURCE_WATERMARK_IDLE_ADVANCE_BROKER_CHECK_TTL.key()));
    }

    return new SourceWatermarkConfig(
        minRecords,
        minOutOfOrderness.toMillis(),
        maxOutOfOrderness.toMillis(),
        quantile,
        idleAdvanceTimeout.toMillis(),
        idleAdvanceSafetyMargin.toMillis(),
        idleAdvanceBrokerCheckTimeout.toMillis(),
        idleAdvanceBrokerCheckTtl.toMillis());
  }

  public record SourceWatermarkConfig(
      int minRecords,
      long minOutOfOrdernessMillis,
      long maxOutOfOrdernessMillis,
      double outOfOrdernessQuantile,
      long idleAdvanceTimeoutMillis,
      long idleAdvanceSafetyMarginMillis,
      long idleAdvanceBrokerCheckTimeoutMillis,
      long idleAdvanceBrokerCheckTtlMillis)
      implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    public SourceWatermarkConfig() {
      // Turn off idle advancement by default.
      this(
          MIN_RECORDS_DEFAULT,
          MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT,
          MAX_OUT_OF_ORDERNESS_MILLIS_DEFAULT,
          OUT_OF_ORDERNESS_QUANTILE_DEFAULT,
          0L,
          0L,
          0L,
          0L);
    }
  }
}
