/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/** Parses a JSON object from string */
@AutoService(AutoRegisterSystemFunction.class)
public class to_jsonb extends ScalarFunction implements AutoRegisterSystemFunction {

  public static final ObjectMapper MAPPER = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(String jsonStr) {
    if (jsonStr == null) {
      return null;
    }
    try {
      return new FlinkJsonType(MAPPER.readTree(jsonStr));
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public FlinkJsonType eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object obj) {
    if (obj == null) {
      return null;
    }

    if (obj instanceof FlinkJsonType jsonTypeObj) {
      return jsonTypeObj;
    }

    return new FlinkJsonType(unboxFlinkToJsonNode(obj));
  }

  JsonNode unboxFlinkToJsonNode(Object obj) {
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

    if (obj instanceof Row[] rowArr) {
      var arrayNode = MAPPER.createArrayNode();
      for (var row : rowArr) {
        if (row == null) {
          arrayNode.addNull();
        } else {
          arrayNode.add(unboxFlinkToJsonNode(row)); // Recursively unbox each row in the array
        }
      }
      return arrayNode;
    }

    return MAPPER.valueToTree(obj); // Directly serialize other types
  }
}
