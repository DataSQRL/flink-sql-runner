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

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.annotation.DataTypeHint;

@AllArgsConstructor
@Getter
public final class ObjectAggAccumulator {

  @DataTypeHint("RAW")
  private final Map<String, JsonNode> objects;

  public void add(String key, JsonNode value) {
    if (key != null) {
      objects.put(key, value);
    }
  }

  public void remove(String key) {
    if (key != null) {
      objects.remove(key);
    }
  }
}
