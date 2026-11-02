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
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;

import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/** The three Jev question primitives, in the order their answers are appended to the output. */
@RequiredArgsConstructor
public enum QuestionKind {
  NOUL("noul", ClassifyPlan.ARG_NOUL),
  CHOICE("choice", ClassifyPlan.ARG_CHOICE),
  SCORE("score", ClassifyPlan.ARG_SCORE);

  /** The SQL argument name, which is also the {@code type} sent to the API. */
  @Getter private final String argName;

  @Getter(AccessLevel.PACKAGE)
  private final int argPos;

  /** Human-readable description of the criteria type this kind expects. */
  String expectedCriteria() {
    return switch (this) {
      case NOUL -> "ROW<`true` T, `false` T>";
      case CHOICE -> "ROW<option T, ...>";
      case SCORE -> "ARRAY<T>";
    };
  }

  /**
   * The answer type for a question of this kind.
   *
   * @param options the Choice option keys, ignored for other kinds
   */
  DataType answerType(List<String> options) {
    return switch (this) {
      case NOUL -> DOUBLE();
      case CHOICE ->
          ROW(
              FIELD("choice", STRING()),
              FIELD("confidence", DOUBLE()),
              FIELD(
                  "probabilities",
                  ROW(
                      options.stream()
                          .map(o -> FIELD(o, DOUBLE()))
                          .toArray(DataTypes.Field[]::new))));
      case SCORE ->
          ROW(
              FIELD("score", DOUBLE()),
              FIELD("confidence", DOUBLE()),
              FIELD("probabilities", ARRAY(DOUBLE())));
    };
  }
}
