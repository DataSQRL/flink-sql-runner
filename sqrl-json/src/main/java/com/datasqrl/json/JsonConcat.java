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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Merges two JSON objects into one. If two objects share the same key, the value from the later
 * object is used.
 */
public class JsonConcat extends ScalarFunction {

  public FlinkJsonType eval(FlinkJsonType json1, FlinkJsonType json2) {
    if (json1 == null || json2 == null) {
      return null;
    }
    try {
      ObjectNode node1 = (ObjectNode) json1.getJson();
      ObjectNode node2 = (ObjectNode) json2.getJson();

      node1.setAll(node2);
      return new FlinkJsonType(node1);
    } catch (Exception e) {
      return null;
    }
  }
}
