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
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Extracts a value from the JSON object based on the provided JSON path. An optional third argument
 * can be provided to specify a default value when the given JSON path does not yield a value for
 * the JSON object.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class jsonb_extract extends ScalarFunction implements AutoRegisterSystemFunction {

  public String eval(FlinkJsonType input, String pathSpec) {
    if (input == null) {
      return null;
    }
    try {
      var jsonNode = input.getJson();
      ReadContext ctx = JsonPath.parse(jsonNode.toString());
      var value = ctx.read(pathSpec);
      if (value == null) {
        return null;
      }
      return value.toString();
    } catch (Exception e) {
      return null;
    }
  }

  public String eval(FlinkJsonType input, String pathSpec, String defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      var parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, String.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public Boolean eval(FlinkJsonType input, String pathSpec, Boolean defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      var parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, Boolean.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public Double eval(FlinkJsonType input, String pathSpec, Double defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      var parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, Double.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public Integer eval(FlinkJsonType input, String pathSpec, Integer defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      var parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, Integer.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }
}
