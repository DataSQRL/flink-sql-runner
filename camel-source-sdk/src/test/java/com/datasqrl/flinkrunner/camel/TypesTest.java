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
package com.datasqrl.flinkrunner.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.Test;

class TypesTest {

  @Test
  void decimalRoundsHalfUpToTheScale() {
    assertThat(Types.decimal("12.345", 18, 2)).isEqualTo(new BigDecimal("12.35"));
  }

  @Test
  void decimalReadsNumericAndTextNodes() {
    assertThat(Types.decimal(new DoubleNode(12.5), 18, 2)).isEqualTo(new BigDecimal("12.50"));
    assertThat(Types.decimal(new TextNode("12.5"), 18, 2)).isEqualTo(new BigDecimal("12.50"));
  }

  @Test
  void decimalOfNothingIsNull() {
    assertThat(Types.decimal((String) null, 18, 2)).isNull();
    assertThat(Types.decimal((JsonNode) null, 18, 2)).isNull();
    assertThat(Types.decimal(NullNode.getInstance(), 18, 2)).isNull();
    assertThat(Types.decimal(MissingNode.getInstance(), 18, 2)).isNull();
  }

  @Test
  void decimalRejectsValuesBeyondThePrecision() {
    assertThatThrownBy(() -> Types.decimal("123.4", 4, 2)).isInstanceOf(ArithmeticException.class);
  }

  @Test
  void instantReadsIsoAndSalesforceOffsets() {
    Instant expected = Instant.parse("2026-11-19T10:15:30.123Z");
    assertThat(Types.instant(new TextNode("2026-11-19T10:15:30.123Z"))).isEqualTo(expected);
    assertThat(Types.instant(new TextNode("2026-11-19T10:15:30.123+0000"))).isEqualTo(expected);
  }

  @Test
  void instantOfNothingIsNull() {
    assertThat(Types.instant(null)).isNull();
    assertThat(Types.instant(NullNode.getInstance())).isNull();
    assertThat(Types.instant(MissingNode.getInstance())).isNull();
  }

  @Test
  void dateUsesThePattern() {
    assertThat(Types.date("20261119", "yyyyMMdd")).isEqualTo(LocalDate.of(2026, 11, 19));
  }

  @Test
  void truncateCutsLongText() {
    assertThat(Types.truncate("abcdef", 3)).isEqualTo("abc...");
    assertThat(Types.truncate("ab", 3)).isEqualTo("ab");
    assertThat(Types.truncate(null, 3)).isEqualTo("null");
  }
}
