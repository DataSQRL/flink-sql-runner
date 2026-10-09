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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

/**
 * Converts external Flink values to JSON. A converter tree is derived once from a {@link
 * LogicalType}, so no type dispatch or reflection happens per row beyond walking arrays.
 *
 * <p>Errors never include the offending value, only its path, because values may contain PII.
 */
@FunctionalInterface
public interface JsonSerializer {

  /** Keeps the scale of {@code DECIMAL} values, since Jackson 2.15+ factories never strip zeros. */
  JsonNodeFactory NODES = JsonNodeFactory.instance;

  /** Converts a value, which may be null, to JSON. */
  JsonNode toJson(Object value);

  /**
   * Creates a serializer for values of the given type.
   *
   * @param type the value type; must have passed {@link ClassifyPlan} validation
   * @param path the location of the value, used in error messages
   */
  static JsonSerializer forType(LogicalType type, String path) {
    var nonNull = nonNull(type, path);

    return value -> value == null ? NODES.nullNode() : nonNull.toJson(value);
  }

  private static JsonSerializer nonNull(LogicalType type, String path) {
    return switch (type.getTypeRoot()) {
      case CHAR, VARCHAR, DATE, TIME_WITHOUT_TIME_ZONE -> value -> NODES.textNode(value.toString());
      case BOOLEAN -> value -> NODES.booleanNode((Boolean) value);
      case TINYINT, SMALLINT, INTEGER -> value -> NODES.numberNode(((Number) value).intValue());
      case BIGINT -> value -> NODES.numberNode(((Number) value).longValue());
      case FLOAT, DOUBLE ->
          value -> {
            var d = ((Number) value).doubleValue();
            if (!Double.isFinite(d)) {
              throw ClassifyException.contract(
                  "non-finite floating point value at '" + path + "' cannot be sent to Jev");
            }
            return value instanceof Float
                ? NODES.numberNode((Float) value)
                : NODES.numberNode((Double) value);
          };
      case DECIMAL -> value -> NODES.numberNode((BigDecimal) value);
      case TIMESTAMP_WITHOUT_TIME_ZONE ->
          value ->
              NODES.textNode(
                  value instanceof Timestamp
                      ? ((Timestamp) value).toLocalDateTime().toString()
                      : ((LocalDateTime) value).toString());
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          value ->
              NODES.textNode(
                  value instanceof Instant
                      ? value.toString()
                      : ((Timestamp) value).toInstant().toString());
      case INTERVAL_YEAR_MONTH ->
          value ->
              NODES.textNode(
                  (value instanceof Period
                          ? (Period) value
                          : Period.ofMonths(((Number) value).intValue()))
                      .normalized()
                      .toString());
      case INTERVAL_DAY_TIME ->
          value ->
              NODES.textNode(
                  value instanceof Duration
                      ? value.toString()
                      : Duration.ofMillis(((Number) value).longValue()).toString());
      case ROW -> row((RowType) type, path);
      case MAP -> {
        var values = forType(((MapType) type).getValueType(), path + "{}");
        yield value -> {
          var node = NODES.objectNode();
          for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
            if (e.getKey() == null) {
              throw ClassifyException.contract("NULL map key at '" + path + "'");
            }
            node.set(e.getKey().toString(), values.toJson(e.getValue()));
          }
          return node;
        };
      }
      case ARRAY -> {
        var elements = forType(((ArrayType) type).getElementType(), path + "[]");
        yield value -> {
          var node = NODES.arrayNode();
          var length = Array.getLength(value);
          for (int i = 0; i < length; i++) {
            node.add(elements.toJson(Array.get(value, i)));
          }
          return node;
        };
      }
      case MULTISET -> {
        var elements = forType(((MultisetType) type).getElementType(), path + "[]");
        yield value -> {
          var node = NODES.arrayNode();
          for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
            var element = elements.toJson(e.getKey());
            var count = ((Number) e.getValue()).intValue();
            for (int i = 0; i < count; i++) {
              node.add(element);
            }
          }
          return node;
        };
      }
      case NULL -> value -> NODES.nullNode();
      default ->
          throw new IllegalArgumentException(
              "Unsupported type " + type.asSummaryString() + " at '" + path + "'");
    };
  }

  private static JsonSerializer row(RowType type, String path) {
    var names = type.getFieldNames();
    var fields = new JsonSerializer[names.size()];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = forType(type.getTypeAt(i), path + "." + names.get(i));
    }

    return value -> {
      var row = (Row) value;
      var node = NODES.objectNode();
      for (int i = 0; i < fields.length; i++) {
        node.set(names.get(i), fields[i].toJson(row.getField(i)));
      }
      return node;
    };
  }
}
