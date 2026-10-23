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
package com.datasqrl.flinkrunner.stdlib.typesafe;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DAY;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Golden tests for every row of the type mapping table. */
class JsonSerializerTest {

  private static String json(DataType type, Object value) {
    return JevClient.toJsonString(
        JsonSerializer.forType(type.getLogicalType(), "state").toJson(value));
  }

  static Stream<Arguments> golden() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("k", 1);
    map.put("n", null);
    Map<String, Integer> multiset = new LinkedHashMap<>();
    multiset.put("x", 2);
    return Stream.of(
        Arguments.of(STRING(), "a \"quoted\" ✓", "\"a \\\"quoted\\\" ✓\""),
        Arguments.of(VARCHAR(10), "v", "\"v\""),
        Arguments.of(CHAR(3), "abc", "\"abc\""),
        Arguments.of(BOOLEAN(), true, "true"),
        Arguments.of(TINYINT(), (byte) 7, "7"),
        Arguments.of(SMALLINT(), (short) -7, "-7"),
        Arguments.of(INT(), 702, "702"),
        Arguments.of(BIGINT(), 9_007_199_254_740_993L, "9007199254740993"),
        Arguments.of(FLOAT(), 1.5f, "1.5"),
        Arguments.of(DOUBLE(), 0.412, "0.412"),
        Arguments.of(DECIMAL(12, 2), new BigDecimal("25000.00"), "25000.00"),
        Arguments.of(DECIMAL(38, 0), new BigDecimal("1E+3"), "1000"),
        Arguments.of(DATE(), LocalDate.of(2026, 1, 31), "\"2026-01-31\""),
        Arguments.of(TIME(3), LocalTime.of(10, 15, 30, 500_000_000), "\"10:15:30.500\""),
        Arguments.of(
            TIMESTAMP(3), LocalDateTime.of(2026, 1, 31, 10, 15, 30), "\"2026-01-31T10:15:30\""),
        Arguments.of(
            TIMESTAMP_LTZ(3),
            Instant.parse("2026-01-31T10:15:30.123Z"),
            "\"2026-01-31T10:15:30.123Z\""),
        Arguments.of(INTERVAL(YEAR(), MONTH()), Period.ofMonths(14), "\"P1Y2M\""),
        Arguments.of(INTERVAL(DAY(), SECOND(3)), Duration.ofMinutes(90), "\"PT1H30M\""),
        Arguments.of(
            ROW(FIELD("a", INT()), FIELD("b", ROW(FIELD("c", STRING())))),
            Row.of(1, Row.of((Object) null)),
            "{\"a\":1,\"b\":{\"c\":null}}"),
        Arguments.of(MAP(STRING(), INT()), map, "{\"k\":1,\"n\":null}"),
        Arguments.of(ARRAY(STRING()), new String[] {"a", null}, "[\"a\",null]"),
        Arguments.of(ARRAY(INT().notNull()), new int[] {1, 2}, "[1,2]"),
        Arguments.of(MULTISET(STRING()), multiset, "[\"x\",\"x\"]"),
        Arguments.of(STRING(), null, "null"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("golden")
  void serializes(DataType type, Object value, String expected) {
    assertThat(json(type, value)).isEqualTo(expected);
  }

  @Test
  void rejectsNonFiniteNumbersWithoutEchoingValues() {
    var type = ROW(FIELD("score", DOUBLE()));
    assertThatThrownBy(() -> json(type, Row.of(Double.NaN)))
        .isInstanceOf(ClassifyException.class)
        .hasMessage(
            "CONTRACT: non-finite floating point value at 'state.score' cannot be sent to Jev");
    assertThatThrownBy(() -> json(FLOAT(), Float.POSITIVE_INFINITY))
        .isInstanceOf(ClassifyException.class);
  }

  @Test
  void rejectsNullMapKeys() {
    Map<String, String> map = new LinkedHashMap<>();
    map.put(null, "v");
    assertThatThrownBy(() -> json(MAP(STRING(), STRING()), map))
        .isInstanceOf(ClassifyException.class)
        .hasMessageContaining("NULL map key at 'state'");
  }
}
