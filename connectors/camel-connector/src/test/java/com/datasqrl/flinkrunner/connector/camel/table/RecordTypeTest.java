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

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.junit.jupiter.api.Test;

class RecordTypeTest {

  public record Inner(String x) {}

  public record Everything(
      String text,
      boolean flag,
      int small,
      long big,
      double ratio,
      BigDecimal amount,
      LocalDate day,
      LocalDateTime local,
      Instant moment,
      List<String> tags,
      Map<String, Long> counts,
      Inner inner) {}

  public record One(String a) {}

  public record Two(String a, String b) {}

  public record Money(BigDecimal amount) {}

  static final DataType EVERYTHING_ROW =
      ROW(
          FIELD("text", STRING()),
          FIELD("flag", BOOLEAN()),
          FIELD("small", INT()),
          FIELD("big", BIGINT()),
          FIELD("ratio", DOUBLE()),
          FIELD("amount", DECIMAL(18, 2)),
          FIELD("day", DATE()),
          FIELD("local", TIMESTAMP(3)),
          FIELD("moment", TIMESTAMP_LTZ(3)),
          FIELD("tags", ARRAY(STRING())),
          FIELD("counts", MAP(STRING(), BIGINT())),
          FIELD("inner", ROW(FIELD("x", STRING()))));

  @Test
  void bridgesEveryDocumentedType() {
    DataType bridged = RecordType.bridge(Everything.class, EVERYTHING_ROW);

    assertThat(bridged.getLogicalType().getTypeRoot()).isEqualTo(LogicalTypeRoot.STRUCTURED_TYPE);
    assertThat(
            ((StructuredType) bridged.getLogicalType())
                .getAttributes().stream().map(StructuredType.StructuredAttribute::getName))
        .containsExactly(
            "text", "flag", "small", "big", "ratio", "amount", "day", "local", "moment", "tags",
            "counts", "inner");
  }

  @Test
  void rejectsAColumnTheRecordLacks() {
    assertThatThrownBy(
            () -> RecordType.bridge(One.class, ROW(FIELD("a", STRING()), FIELD("b", STRING()))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'b'")
        .hasMessageContaining("One");
  }

  @Test
  void rejectsAComponentTheTableLacks() {
    assertThatThrownBy(() -> RecordType.bridge(Two.class, ROW(FIELD("a", STRING()))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'b'")
        .hasMessageContaining("Two");
  }

  @Test
  void rejectsColumnsOutOfComponentOrder() {
    assertThatThrownBy(
            () -> RecordType.bridge(Two.class, ROW(FIELD("b", STRING()), FIELD("a", STRING()))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'b'")
        .hasMessageContaining("Two");
  }

  @Test
  void rejectsAnIncompatibleColumnType() {
    assertThatThrownBy(() -> RecordType.bridge(Money.class, ROW(FIELD("amount", STRING()))))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'amount'")
        .hasMessageContaining("STRING")
        .hasMessageContaining("BigDecimal");
  }
}
