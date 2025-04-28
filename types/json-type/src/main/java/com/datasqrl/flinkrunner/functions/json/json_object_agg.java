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
package com.datasqrl.flinkrunner.functions.json;

import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import com.datasqrl.flinkrunner.types.json.FlinkJsonTypeSerializer;
import java.util.LinkedHashMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Aggregation function that merges JSON objects into a single JSON object. If two JSON objects
 * share the same field name, the value of the later one is used in the aggregated result.
 */
@FunctionHint(
    output =
        @DataTypeHint(
            value = "RAW",
            bridgedTo = FlinkJsonType.class,
            rawSerializer = FlinkJsonTypeSerializer.class))
public class json_object_agg extends AggregateFunction<Object, ObjectAgg> {

  private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  @Override
  public ObjectAgg createAccumulator() {
    return new ObjectAgg(new LinkedHashMap<>());
  }

  public void accumulate(ObjectAgg accumulator, String key, String value) {
    accumulateObject(accumulator, key, value);
  }

  public void accumulate(
      ObjectAgg accumulator, String key, @DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
    if (value instanceof FlinkJsonType type) {
      accumulateObject(accumulator, key, type.getJson());
    } else {
      accumulator.add(key, mapper.getNodeFactory().pojoNode(value));
    }
  }

  public void accumulate(ObjectAgg accumulator, String key, Double value) {
    accumulateObject(accumulator, key, value);
  }

  public void accumulate(ObjectAgg accumulator, String key, Long value) {
    accumulateObject(accumulator, key, value);
  }

  public void accumulate(ObjectAgg accumulator, String key, Integer value) {
    accumulateObject(accumulator, key, value);
  }

  public void accumulateObject(ObjectAgg accumulator, String key, Object value) {
    accumulator.add(key, mapper.getNodeFactory().pojoNode(value));
  }

  public void retract(ObjectAgg accumulator, String key, String value) {
    retractObject(accumulator, key);
  }

  public void retract(
      ObjectAgg accumulator, String key, @DataTypeHint(inputGroup = InputGroup.ANY) Object value) {
    retractObject(accumulator, key);
  }

  public void retract(ObjectAgg accumulator, String key, Double value) {
    retractObject(accumulator, key);
  }

  public void retract(ObjectAgg accumulator, String key, Long value) {
    retractObject(accumulator, key);
  }

  public void retract(ObjectAgg accumulator, String key, Integer value) {
    retractObject(accumulator, key);
  }

  public void retractObject(ObjectAgg accumulator, String key) {
    accumulator.remove(key);
  }

  public void merge(ObjectAgg accumulator, java.lang.Iterable<ObjectAgg> iterable) {
    iterable.forEach(o -> accumulator.getObjects().putAll(o.getObjects()));
  }

  @Override
  public FlinkJsonType getValue(ObjectAgg accumulator) {
    var objectNode = mapper.createObjectNode();
    accumulator.getObjects().forEach(objectNode::putPOJO);
    return new FlinkJsonType(objectNode);
  }
}
