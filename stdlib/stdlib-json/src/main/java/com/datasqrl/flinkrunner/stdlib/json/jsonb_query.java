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
import com.jayway.jsonpath.JsonPath;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * For a given JSON object, executes a JSON path query against the object and returns the result as
 * string.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class jsonb_query extends ScalarFunction implements AutoRegisterSystemFunction {
  static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public String eval(FlinkJsonType input, String pathSpec) {
    if (input == null) {
      return null;
    }
    try {
      var jsonNode = input.getJson();
      var ctx = JsonPath.parse(jsonNode.toString());
      var result = ctx.read(pathSpec);
      return mapper.writeValueAsString(result); // Convert the result back to JSON string
    } catch (Exception e) {
      return null;
    }
  }
}
