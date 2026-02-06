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

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import java.util.LinkedList;
import java.util.List;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/** Aggregation function that aggregates JSON objects into a JSON array. */
@AutoService(AutoRegisterSystemFunction.class)
public class jsonb_array_agg extends AggregateFunction<FlinkJsonType, ArrayAggAccumulator>
    implements AutoRegisterSystemFunction {

  private static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  @Override
  public ArrayAggAccumulator createAccumulator() {
    return new ArrayAggAccumulator(new LinkedList<>(), new LinkedList<>());
  }

  public void accumulate(ArrayAggAccumulator acc, String value) {
    acc.add(mapper.getNodeFactory().textNode(value));
  }

  public void accumulate(ArrayAggAccumulator acc, FlinkJsonType value) {
    acc.add(value == null ? null : value.json);
  }

  public void accumulate(ArrayAggAccumulator acc, Double value) {
    acc.add(mapper.getNodeFactory().numberNode(value));
  }

  public void accumulate(ArrayAggAccumulator acc, Long value) {
    acc.add(mapper.getNodeFactory().numberNode(value));
  }

  public void accumulate(ArrayAggAccumulator acc, Integer value) {
    acc.add(mapper.getNodeFactory().numberNode(value));
  }

  public void retract(ArrayAggAccumulator acc, String value) {
    var nodeVal = mapper.getNodeFactory().textNode(value);
    if (!acc.remove(nodeVal)) {
      acc.addRetract(nodeVal);
    }
  }

  public void retract(ArrayAggAccumulator acc, FlinkJsonType value) {
    var finalVal = value == null ? null : value.json;
    if (!acc.remove(finalVal)) {
      acc.addRetract(finalVal);
    }
  }

  public void retract(ArrayAggAccumulator acc, Double value) {
    var nodeVal = mapper.getNodeFactory().numberNode(value);
    if (!acc.getElements().remove(nodeVal)) {
      acc.addRetract(nodeVal);
    }
  }

  public void retract(ArrayAggAccumulator acc, Long value) {
    var nodeVal = mapper.getNodeFactory().numberNode(value);
    if (!acc.getElements().remove(nodeVal)) {
      acc.addRetract(nodeVal);
    }
  }

  public void retract(ArrayAggAccumulator acc, Integer value) {
    var nodeVal = mapper.getNodeFactory().numberNode(value);
    if (!acc.getElements().remove(nodeVal)) {
      acc.addRetract(nodeVal);
    }
  }

  public void merge(ArrayAggAccumulator acc, Iterable<ArrayAggAccumulator> iterable) {
    for (ArrayAggAccumulator otherAcc : iterable) {
      acc.getElements().addAll(otherAcc.getElements());
      acc.getRetractElements().addAll(otherAcc.getRetractElements());
    }

    List<JsonNode> newRetractBuffer = new LinkedList<>();
    for (JsonNode elem : acc.getRetractElements()) {
      if (!acc.remove(elem)) {
        newRetractBuffer.add(elem);
      }
    }

    acc.getRetractElements().clear();
    acc.getRetractElements().addAll(newRetractBuffer);
  }

  @Override
  public FlinkJsonType getValue(ArrayAggAccumulator acc) {
    var arrayNode = mapper.createArrayNode();
    for (Object o : acc.getElements()) {
      if (o instanceof FlinkJsonType) {
        arrayNode.add(((FlinkJsonType) o).json);
      } else {
        arrayNode.addPOJO(o);
      }
    }
    return new FlinkJsonType(arrayNode);
  }
}
