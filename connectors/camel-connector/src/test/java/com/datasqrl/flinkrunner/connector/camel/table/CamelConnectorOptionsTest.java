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
package com.datasqrl.flinkrunner.connector.camel.table;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

class CamelConnectorOptionsTest {

  @Test
  void rejectsAnOptionOfAnotherMode() {
    assertThatThrownBy(
            () -> CamelConnectorOptions.validateFor(SourceMode.CURSOR, Set.of("file.order"), "a.B"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'file.order'")
        .hasMessageContaining("cursor")
        .hasMessageContaining("a.B");
  }

  @Test
  void rejectsPollingOptionsForAckSources() {
    assertThatThrownBy(
            () -> CamelConnectorOptions.validateFor(SourceMode.ACK, Set.of("poll.interval"), "a.B"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'poll.interval'")
        .hasMessageContaining("ack");
  }

  @Test
  void acceptsSharedAndComponentOptions() {
    assertThatCode(
            () ->
                CamelConnectorOptions.validateFor(
                    SourceMode.ACK,
                    Set.of("fetch.timeout", "scan.bounded", "camel.component.jms.x"),
                    "a.B"))
        .doesNotThrowAnyException();
  }
}
