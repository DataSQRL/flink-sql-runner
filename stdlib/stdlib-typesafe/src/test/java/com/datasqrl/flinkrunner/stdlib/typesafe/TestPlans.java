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
package com.datasqrl.flinkrunner.stdlib.typesafe;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.types.ColumnList;

/** Builds {@link ClassifyPlan}s without a planner, from literal argument values. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TestPlans {

  /** Input: id, case_file, q_noul, q_choice, q_score. */
  static final DataType INPUT =
      ROW(
          FIELD("id", STRING()),
          FIELD("case_file", ROW(FIELD("amount", DECIMAL(12, 2)), FIELD("notes", STRING()))),
          FIELD(
              "q_noul",
              ROW(
                  FIELD("instructions", STRING()),
                  FIELD("criteria", ROW(FIELD("true", STRING()), FIELD("false", STRING()))))),
          FIELD(
              "q_choice",
              ROW(
                  FIELD("instructions", STRING()),
                  FIELD("criteria", ROW(FIELD("a", STRING()), FIELD("b", STRING()))))),
          FIELD(
              "q_score", ROW(FIELD("instructions", STRING()), FIELD("criteria", ARRAY(STRING())))));

  static ClassifyPlan plan(ErrorPolicy policy) {
    return ClassifyPlan.fromCallContext(
        context(
            INPUT,
            ColumnList.of("case_file"),
            ColumnList.of("q_noul"),
            ColumnList.of("q_choice"),
            ColumnList.of("q_score"),
            "jev-1.13.0",
            policy.name()));
  }

  static CallContext context(DataType input, Object... scalarArgs) {
    var values = new Object[scalarArgs.length + 1];
    System.arraycopy(scalarArgs, 0, values, 1, scalarArgs.length);
    var types = Arrays.asList(new DataType[values.length]);
    types.set(0, input);
    return new CallContext() {
      @Override
      public DataTypeFactory getDataTypeFactory() {
        throw new UnsupportedOperationException();
      }

      @Override
      public FunctionDefinition getFunctionDefinition() {
        return new typesafe_classify();
      }

      @Override
      public boolean isArgumentLiteral(int pos) {
        return true;
      }

      @Override
      public boolean isArgumentNull(int pos) {
        return values[pos] == null;
      }

      @Override
      public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        return Optional.ofNullable(values[pos]).filter(clazz::isInstance).map(clazz::cast);
      }

      @Override
      public String getName() {
        return "TypeSafeClassify";
      }

      @Override
      public List<DataType> getArgumentDataTypes() {
        return types;
      }

      @Override
      public Optional<DataType> getOutputDataType() {
        return Optional.empty();
      }

      @Override
      public boolean isGroupedAggregation() {
        return false;
      }
    };
  }
}
