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
package com.datasqrl.flinkrunner.stdlib.json;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

@Slf4j
public class JsonFunctions {

  private static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

  public static final to_jsonb TO_JSON = new to_jsonb();
  public static final jsonb_to_string JSON_TO_STRING = new jsonb_to_string();
  public static final jsonb_object JSON_OBJECT = new jsonb_object();
  public static final jsonb_array JSON_ARRAY = new jsonb_array();
  public static final jsonb_extract JSON_EXTRACT = new jsonb_extract();
  public static final jsonb_query JSON_QUERY = new jsonb_query();
  public static final jsonb_exists JSON_EXISTS = new jsonb_exists();
  public static final jsonb_array_agg JSON_ARRAYAGG = new jsonb_array_agg();
  public static final jsonb_object_agg JSON_OBJECTAGG = new jsonb_object_agg();
  public static final jsonb_concat JSON_CONCAT = new jsonb_concat();

  static ArgumentTypeStrategy createJsonArgumentTypeStrategy(DataTypeFactory typeFactory) {
    return InputTypeStrategies.or(
        SpecificInputTypeStrategies.JSON_ARGUMENT,
        InputTypeStrategies.explicit(createJsonType(typeFactory)));
  }

  static DataType createJsonType(DataTypeFactory typeFactory) {
    var dataType = DataTypes.of(FlinkJsonType.class).toDataType(typeFactory);
    return dataType;
  }

  /**
   * Converts a Flink value to a {@link JsonNode}, recursing into {@link Row}s, arrays, and maps.
   *
   * <ul>
   *   <li>{@code null} becomes a JSON {@code null}.
   *   <li>{@link FlinkJsonType} is embedded as its wrapped JSON value.
   *   <li>{@link Row} becomes an object keyed by field name, or by {@code f0}, {@code f1}, ... if
   *       the row has no field names.
   *   <li>Object arrays become arrays.
   *   <li>Maps become objects, with keys serialized by Jackson. Entries with a {@code null} key are
   *       skipped, because JSON does not allow them.
   *   <li>Any other value is serialized by Jackson.
   * </ul>
   *
   * @param obj the Flink value to convert, may be {@code null}
   * @return the JSON representation of {@code obj}, never {@code null}
   */
  static JsonNode unboxFlinkToJsonNode(Object obj) {
    if (obj == null) {
      return MAPPER.getNodeFactory().nullNode();
    }

    if (obj instanceof FlinkJsonType jsonTypeObj) {
      // Use the wrapped JSON value, otherwise the wrapper would be serialized as {"json": ...}
      return jsonTypeObj.getJson();
    }

    if (obj instanceof Row row) {
      var objectNode = MAPPER.createObjectNode();

      var fieldNames = row.getFieldNames(true);
      if (fieldNames == null) {
        // No named fields, use position
        for (var i = 0; i < row.getArity(); i++) {
          var field = row.getField(i);
          objectNode.set("f" + i, unboxFlinkToJsonNode(field));
        }
      } else {
        for (var fieldName : fieldNames) {
          var field = row.getField(fieldName);
          objectNode.set(fieldName, unboxFlinkToJsonNode(field)); // Recursively unbox each field
        }
      }

      return objectNode;
    }

    if (obj instanceof Object[] arr) {
      var arrayNode = MAPPER.createArrayNode();
      for (var element : arr) {
        arrayNode.add(unboxFlinkToJsonNode(element)); // Recursively unbox each array element
      }
      return arrayNode;
    }

    if (obj instanceof Map<?, ?> map) {
      // Recursively unbox each map value, but let Jackson serialize the keys as before
      var unboxed = new LinkedHashMap<Object, JsonNode>();
      for (var entry : map.entrySet()) {
        if (entry.getKey() == null) {
          // JSON object keys must be strings, and Jackson throws on a null key, which would fail
          // the job, so drop the entry and leave catching it to pipeline validation
          log.warn("Skipping map entry with a NULL key, because JSON object keys cannot be null");
          continue;
        }
        unboxed.put(entry.getKey(), unboxFlinkToJsonNode(entry.getValue()));
      }
      return MAPPER.valueToTree(unboxed);
    }

    return MAPPER.valueToTree(obj); // Directly serialize other types
  }
}
