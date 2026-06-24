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

import java.util.Optional;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

public class RateLimitOptions {

  public static final ConfigOption<Double> SCAN_RATE_LIMIT_RECORDS_PER_SECOND =
      ConfigOptions.key("scan.rate-limit.records-per-second")
          .doubleType()
          .noDefaultValue()
          .withDescription(
              "Optional maximum number of records per second emitted by the Kafka source.");

  public static Optional<Double> scanRateLimitRecordsPerSecond(ReadableConfig tableOptions) {
    Optional<Double> recordsPerSecond =
        tableOptions.getOptional(SCAN_RATE_LIMIT_RECORDS_PER_SECOND);

    recordsPerSecond.ifPresent(
        value -> {
          if (!Double.isFinite(value) || value <= 0D) {
            throw new ValidationException(
                "'%s' must be a finite number greater than 0."
                    .formatted(SCAN_RATE_LIMIT_RECORDS_PER_SECOND.key()));
          }
        });

    return recordsPerSecond;
  }
}
