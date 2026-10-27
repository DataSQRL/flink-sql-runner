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

import static com.datasqrl.flinkrunner.connector.kafka.KeyFieldsOptions.SCAN_KEY_FIELDS_READ_FROM_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.table.SafeKafkaDynamicTableFactory;
import org.apache.flink.streaming.connectors.kafka.table.SafeUpsertKafkaDynamicTableFactory;
import org.junit.jupiter.api.Test;

class KeyFieldsOptionsTest {

  @Test
  void readsKeyFieldsFromValueByDefault() {
    assertThat(new Configuration().get(SCAN_KEY_FIELDS_READ_FROM_VALUE)).isTrue();
  }

  @Test
  void isOnlySupportedByNonUpsertConnector() {
    assertThat(new SafeKafkaDynamicTableFactory().optionalOptions())
        .contains(SCAN_KEY_FIELDS_READ_FROM_VALUE);
    assertThat(new SafeUpsertKafkaDynamicTableFactory().optionalOptions())
        .doesNotContain(SCAN_KEY_FIELDS_READ_FROM_VALUE);
  }
}
