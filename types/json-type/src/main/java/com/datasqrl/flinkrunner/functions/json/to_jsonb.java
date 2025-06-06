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
package com.datasqrl.flinkrunner.functions.json;

import com.datasqrl.flinkrunner.functions.AutoRegisterSystemFunction;
import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
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

  public static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(String json) {
    if (json == null) {
      return null;
    }
    try {
      return new FlinkJsonType(mapper.readTree(json));
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  public FlinkJsonType eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object json) {
    if (json == null) {
      return null;
    }
    if (json instanceof FlinkJsonType) {
      return (FlinkJsonType) json;
    }

    return new FlinkJsonType(unboxFlinkToJsonNode(json));
  }

  JsonNode unboxFlinkToJsonNode(Object json) {
    if (json instanceof Row) {
      var row = (Row) json;
      var objectNode = mapper.createObjectNode();
      var fieldNames =
          row.getFieldNames(true).toArray(new String[0]); // Get field names in an array
      for (String fieldName : fieldNames) {
        var field = row.getField(fieldName);
        objectNode.set(fieldName, unboxFlinkToJsonNode(field)); // Recursively unbox each field
      }
      return objectNode;
    } else if (json instanceof Row[]) {
      var rows = (Row[]) json;
      var arrayNode = mapper.createArrayNode();
      for (Row row : rows) {
        if (row == null) {
          arrayNode.addNull();
        } else {
          arrayNode.add(unboxFlinkToJsonNode(row)); // Recursively unbox each row in the array
        }
      }
      return arrayNode;
    }
    return mapper.valueToTree(json); // Directly serialize other types
  }
}
