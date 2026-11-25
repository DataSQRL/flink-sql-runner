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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

class CursorStartTest {

  /** A composite cursor, written {@code <instant>|<id>}. */
  public record Stamp(Instant at, String id) {
    public static Stamp parse(String text) {
      String[] parts = text.split("\\|");
      return new Stamp(Instant.parse(parts[0]), parts[1]);
    }
  }

  public record Unparseable(long value) {}

  @Test
  void parsesNumbers() {
    assertThat(CursorStart.parse("20", Long.class)).isEqualTo(20L);
    assertThat(CursorStart.parse("7", Integer.class)).isEqualTo(7);
  }

  @Test
  void parsesTimes() {
    assertThat(CursorStart.parse("2026-11-20T10:15:30Z", Instant.class))
        .isEqualTo(Instant.parse("2026-11-20T10:15:30Z"));
    assertThat(CursorStart.parse("2026-11-20", LocalDate.class))
        .isEqualTo(LocalDate.of(2026, 11, 20));
    assertThat(CursorStart.parse("2026-11-20T10:15:30", LocalDateTime.class))
        .isEqualTo(LocalDateTime.of(2026, 11, 20, 10, 15, 30));
  }

  @Test
  void keepsStrings() {
    assertThat(CursorStart.parse("abc", String.class)).isEqualTo("abc");
  }

  @Test
  void parsesAPositionWithAStaticParseMethod() {
    assertThat(CursorStart.parse("2026-11-20T10:15:30Z|42", Stamp.class))
        .isEqualTo(new Stamp(Instant.parse("2026-11-20T10:15:30Z"), "42"));
  }

  @Test
  void rejectsAPositionTypeItCannotParse() {
    assertThatThrownBy(() -> CursorStart.parse("1", Unparseable.class))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'cursor.start'")
        .hasMessageContaining(Unparseable.class.getName());
  }

  @Test
  void rejectsAMalformedValue() {
    assertThatThrownBy(() -> CursorStart.parse("soon", Instant.class))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'cursor.start'")
        .hasMessageContaining("'soon'");
  }
}
