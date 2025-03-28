/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.json;

import static com.datasqrl.json.JsonFunctions.createJsonArgumentTypeStrategy;
import static com.datasqrl.json.JsonFunctions.createJsonType;

import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/** Creates a JSON array from the list of JSON objects and scalar values. */
@AutoService(StandardLibraryFunction.class)
public class JsonArray extends ScalarFunction implements StandardLibraryFunction {
  private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(Object... objects) {
    ArrayNode arrayNode = mapper.createArrayNode();

    for (Object value : objects) {
      if (value instanceof FlinkJsonType) {
        FlinkJsonType type = (FlinkJsonType) value;
        arrayNode.add(type.json);
      } else {
        arrayNode.addPOJO(value);
      }
    }

    return new FlinkJsonType(arrayNode);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    InputTypeStrategy inputTypeStrategy =
        InputTypeStrategies.varyingSequence(createJsonArgumentTypeStrategy(typeFactory));

    return TypeInference.newBuilder()
        .inputTypeStrategy(inputTypeStrategy)
        .outputTypeStrategy(TypeStrategies.explicit(createJsonType(typeFactory)))
        .build();
  }
}
