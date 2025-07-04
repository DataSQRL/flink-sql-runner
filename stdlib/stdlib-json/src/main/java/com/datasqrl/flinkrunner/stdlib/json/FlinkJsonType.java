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

import java.util.Objects;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.annotation.DataTypeHint;

@DataTypeHint(
    value = "RAW",
    bridgedTo = FlinkJsonType.class,
    rawSerializer = FlinkJsonTypeSerializer.class)
public class FlinkJsonType {

  public JsonNode json;

  public FlinkJsonType(JsonNode json) {
    this.json = json;
  }

  public JsonNode getJson() {
    return json;
  }

  @Override
  public int hashCode() {
    return Objects.hash(json);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof FlinkJsonType)) return false;
    var other = (FlinkJsonType) obj;
    return Objects.equals(json, other.json);
  }
}
