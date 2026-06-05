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

import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaAdminIdleAdvanceReadinessCheckerTest {

  @Test
  void testCachesBrokerCheckUntilConfiguredTtlExpires() {
    var checker =
        new CountingReadinessChecker(
            new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 10_000));

    assertThat(checker.isReady(1000)).isTrue();
    assertThat(checker.isReady(10_999)).isTrue();
    assertThat(checker.checkCount).isEqualTo(1);

    assertThat(checker.isReady(11_000)).isFalse();
    assertThat(checker.checkCount).isEqualTo(2);
  }

  private static final class CountingReadinessChecker
      extends KafkaAdminIdleAdvanceReadinessChecker {

    private int checkCount;

    private CountingReadinessChecker(SourceWatermarkConfig config) {
      super(new Properties(), List.of("topic"), config);
    }

    @Override
    boolean checkBrokerAndTopicTimestampType() {
      checkCount++;
      return checkCount == 1;
    }
  }
}
