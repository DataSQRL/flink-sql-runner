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

import static com.datasqrl.flinkrunner.connector.kafka.RateLimitOptions.SCAN_RATE_LIMIT_RECORDS_PER_SECOND;
import static com.datasqrl.flinkrunner.connector.kafka.RateLimitOptions.scanRateLimitRecordsPerSecond;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RateLimitOptionsTest {

  @Test
  void returnsEmptyWhenRateLimitIsNotConfigured() {
    assertThat(scanRateLimitRecordsPerSecond(new Configuration())).isEmpty();
  }

  @Test
  void returnsConfiguredRateLimit() {
    Configuration configuration = new Configuration();
    configuration.set(SCAN_RATE_LIMIT_RECORDS_PER_SECOND, 123);

    assertThat(scanRateLimitRecordsPerSecond(configuration)).contains(123);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void rejectsInvalidRateLimits(int recordsPerSecond) {
    Configuration configuration = new Configuration();
    configuration.set(SCAN_RATE_LIMIT_RECORDS_PER_SECOND, recordsPerSecond);

    assertThatThrownBy(() -> scanRateLimitRecordsPerSecond(configuration))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(SCAN_RATE_LIMIT_RECORDS_PER_SECOND.key());
  }
}
