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
package com.datasqrl.types.json.functions;

import static com.datasqrl.types.json.functions.JsonFunctions.createJsonArgumentTypeStrategy;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.datasqrl.types.json.FlinkJsonType;
import com.google.auto.service.AutoService;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Creates a JSON object from key-value pairs, where the key is mapped to a field with the
 * associated value. Key-value pairs are provided as a list of even length, with the first element
 * of each pair being the key and the second being the value. If multiple key-value pairs have the
 * same key, the last pair is added to the JSON object.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class JsonObject extends ScalarFunction implements AutoRegisterSystemFunction {
  static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(Object... objects) {
    if (objects.length % 2 != 0) {
      throw new IllegalArgumentException("Arguments should be in key-value pairs");
    }

    ObjectNode objectNode = mapper.createObjectNode();

    for (int i = 0; i < objects.length; i += 2) {
      if (!(objects[i] instanceof String)) {
        throw new IllegalArgumentException("Key must be a string");
      }
      String key = (String) objects[i];
      Object value = objects[i + 1];
      if (value instanceof FlinkJsonType) {
        FlinkJsonType type = (FlinkJsonType) value;
        objectNode.put(key, type.json);
      } else {
        objectNode.putPOJO(key, value);
      }
    }

    return new FlinkJsonType(objectNode);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    InputTypeStrategy anyJsonCompatibleArg =
        InputTypeStrategies.repeatingSequence(createJsonArgumentTypeStrategy(typeFactory));

    InputTypeStrategy inputTypeStrategy =
        InputTypeStrategies.compositeSequence().finishWithVarying(anyJsonCompatibleArg);

    return TypeInference.newBuilder()
        .inputTypeStrategy(inputTypeStrategy)
        .outputTypeStrategy(
            TypeStrategies.explicit(DataTypes.of(FlinkJsonType.class).toDataType(typeFactory)))
        .build();
  }
}
