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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Collectors;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Type inference and planning-time validation of {@link typesafe_classify} in the real planner. */
class TypeSafeClassifyPlanningTest {

  private TableEnvironment tEnv;

  @BeforeEach
  void setUp() {
    tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.executeSql(
        "CREATE TEMPORARY FUNCTION TypeSafeClassify AS '"
            + typesafe_classify.class.getName()
            + "'");
    tEnv.executeSql(
        """
        CREATE TABLE loans (
          id STRING,
          amount DECIMAL(12, 2),
          purpose STRING,
          raw_bytes BYTES,
          tags MAP<INT, STRING>,
          submitted_at TIMESTAMP_LTZ(3),
          WATERMARK FOR submitted_at AS submitted_at - INTERVAL '10' SECOND
        ) WITH ('connector' = 'datagen')""");
    tEnv.executeSql(
        """
        CREATE TEMPORARY VIEW apps AS
        SELECT
          id,
          submitted_at,
          CAST(ROW(ROW(amount, purpose), 0.4) AS ROW<
            loan ROW<amount DECIMAL(12, 2), purpose STRING>, dti DECIMAL(6, 3)>) AS case_file,
          CAST(ROW('Verified?', ROW('yes', 'no'))
            AS ROW<instructions STRING, criteria ROW<`true` STRING, `false` STRING>>) AS q_noul,
          CAST(ROW('Purpose?', ROW('Debts', 'Home', CAST(NULL AS STRING)))
            AS ROW<instructions STRING,
                   criteria ROW<debt STRING, home STRING, other STRING>>) AS q_choice,
          CAST(ROW('Scrutiny?', ARRAY['low', 'mid', 'high'])
            AS ROW<instructions STRING, criteria ARRAY<STRING>>) AS q_score,
          CAST(ROW('Purpose?', ROW('Only'))
            AS ROW<instructions STRING, criteria ROW<single STRING>>) AS q_one_option,
          CAST(ROW(1, ARRAY['a', 'b'])
            AS ROW<instructions INT, criteria ARRAY<STRING>>) AS q_int_instructions,
          CAST(ROW('x', ARRAY['a', 'b']) AS ROW<instructions STRING, crit ARRAY<STRING>>) AS q_bad,
          raw_bytes,
          tags,
          CAST(ROW('Q', ROW('a', 'b')) AS ROW<instructions STRING,
            criteria ROW<a STRING, b STRING>>) AS q_score_answer_clash,
          CAST(NULL AS STRING) AS q_choice_answer
        FROM loans""");
    tEnv.executeSql(
        """
        CREATE TEMPORARY VIEW clean_input AS
        SELECT id, submitted_at, case_file, q_noul, q_choice, q_score FROM apps""");
    tEnv.executeSql(
        """
        CREATE TEMPORARY VIEW no_time_input AS
        SELECT id, CAST(submitted_at AS TIMESTAMP_LTZ(3)) AS ts, case_file, q_noul, q_choice,
               q_score
        FROM apps""");
  }

  private ResolvedSchema schema(String args) {
    return tEnv.sqlQuery("SELECT * FROM TypeSafeClassify(" + args + ")").getResolvedSchema();
  }

  private static String types(ResolvedSchema schema) {
    return schema.getColumns().stream()
        .map(c -> c.getName() + " " + c.getDataType())
        .collect(Collectors.joining("\n"));
  }

  @Test
  void infersTypedAnswersForAllKinds() {
    var schema =
        schema(
            """
            input => TABLE clean_input,
            state => DESCRIPTOR(case_file),
            noul => DESCRIPTOR(q_noul),
            choice => DESCRIPTOR(q_choice),
            score => DESCRIPTOR(q_score),
            model => 'jev-1.13.0',
            on_time => DESCRIPTOR(submitted_at)""");

    assertThat(schema.getColumnNames())
        .containsExactly(
            "id",
            "submitted_at",
            "case_file",
            "q_noul",
            "q_choice",
            "q_score",
            "q_noul_answer",
            "q_choice_answer",
            "q_score_answer",
            "_model",
            "rowtime");
    assertThat(types(schema))
        .contains("q_noul_answer DOUBLE")
        .contains(
            "q_choice_answer ROW<`choice` STRING, `confidence` DOUBLE, `probabilities` "
                + "ROW<`debt` DOUBLE, `home` DOUBLE, `other` DOUBLE>>")
        .contains(
            "q_score_answer ROW<`score` DOUBLE, `confidence` DOUBLE, "
                + "`probabilities` ARRAY<DOUBLE>>")
        .contains("_model STRING NOT NULL")
        .doesNotContain("_error");

    var rowtime = schema.getColumn("rowtime").orElseThrow();
    assertThat(LogicalTypeChecks.isRowtimeAttribute(rowtime.getDataType().getLogicalType()))
        .isTrue();
  }

  @Test
  void answersFollowKindThenDescriptorOrder() {
    var schema =
        schema(
            """
            input => TABLE clean_input,
            state => DESCRIPTOR(case_file),
            score => DESCRIPTOR(q_score),
            choice => DESCRIPTOR(q_choice, q_noul),
            on_time => DESCRIPTOR(submitted_at)""");
    assertThat(schema.getColumnNames())
        .containsSubsequence(
            "q_choice_answer", "q_noul_answer", "q_score_answer", "_model", "rowtime");
    // A Noul-shaped ROW is also a valid Choice with the options `true` and `false`.
    assertThat(types(schema))
        .contains(
            "q_noul_answer ROW<`choice` STRING, `confidence` DOUBLE, `probabilities` "
                + "ROW<`true` DOUBLE, `false` DOUBLE>>");
  }

  @Test
  void errorColumnDependsOnErrorPolicy() {
    var base =
        """
        input => TABLE clean_input,
        state => DESCRIPTOR(case_file),
        noul => DESCRIPTOR(q_noul),
        on_time => DESCRIPTOR(submitted_at)""";
    assertThat(schema(base + ", on_error => 'NULL'").getColumn("_error"))
        .hasValueSatisfying(
            c -> assertThat(c.getDataType().getLogicalType().isNullable()).isTrue());
    assertThat(schema(base + ", on_error => 'FAIL'").getColumn("_error")).isEmpty();
    assertThat(schema(base).getColumn("_error")).isEmpty();

    assertThatThrownBy(
            () ->
                tEnv.sqlQuery(
                    "SELECT _error FROM TypeSafeClassify(" + base + ", on_error => 'FAIL')"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("_error");
  }

  @Test
  void inputWithoutEventTimeNeedsNoOnTime() {
    var schema =
        schema(
            """
            input => TABLE no_time_input,
            state => DESCRIPTOR(case_file),
            noul => DESCRIPTOR(q_noul)""");
    assertThat(schema.getColumnNames()).doesNotContain("rowtime").endsWith("_model");
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          missing column        | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(q_nope)                          | `noul => DESCRIPTOR(q_nope)`: column 'q_nope' does not exist in the input table
          missing state column  | state => DESCRIPTOR(nope), noul => DESCRIPTOR(q_noul)                               | `state => DESCRIPTOR(nope)`: column 'nope' does not exist
          two state columns     | state => DESCRIPTOR(case_file, id), noul => DESCRIPTOR(q_noul)                      | exactly one state column is required, but 2 were given
          state reused          | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(case_file)                       | column 'case_file' is already used by `state`
          question reused       | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(q_noul), choice => DESCRIPTOR(q_noul) | column 'q_noul' is already used by `noul`
          duplicate in one arg  | state => DESCRIPTOR(case_file), score => DESCRIPTOR(q_score, q_score)               | column 'q_score' is already used by `score`
          no questions          | state => DESCRIPTOR(case_file)                                                      | at least one question is required
          state not text        | state => DESCRIPTOR(submitted_at), noul => DESCRIPTOR(q_noul)                       | column 'submitted_at' must have a text-like type
          state binary          | state => DESCRIPTOR(raw_bytes), noul => DESCRIPTOR(q_noul)                               | `state => DESCRIPTOR(raw_bytes)`: column 'raw_bytes' has unsupported type BYTES at 'state'. Jev accepts text only
          state map int keys    | state => DESCRIPTOR(tags), noul => DESCRIPTOR(q_noul)                               | column 'tags' has unsupported type MAP<INT, STRING>
          choice gets array     | state => DESCRIPTOR(case_file), choice => DESCRIPTOR(q_score)                       | `choice => DESCRIPTOR(q_score)`: column 'q_score' must have criteria of type ROW<option T, ...>, but found ARRAY<STRING>. Did you mean `score`?
          noul gets choice      | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(q_choice)                        | `noul => DESCRIPTOR(q_choice)`: column 'q_choice' must have criteria of type ROW<`true` T, `false` T>
          score gets row        | state => DESCRIPTOR(case_file), score => DESCRIPTOR(q_noul)                         | must have criteria of type ARRAY<T>, but found ROW<`true` STRING, `false` STRING>. Did you mean `noul`?
          one choice option     | state => DESCRIPTOR(case_file), choice => DESCRIPTOR(q_one_option)                  | Choice criteria need between 2 and 255 options, but found 1
          instructions not text | state => DESCRIPTOR(case_file), score => DESCRIPTOR(q_int_instructions)             | column 'q_int_instructions' must have a text-like type (STRING, ROW, ARRAY or MAP<STRING, ...>) at 'instructions', but found INT
          bad question shape    | state => DESCRIPTOR(case_file), score => DESCRIPTOR(q_bad)                          | column 'q_bad' must be of type ROW<instructions T, criteria ARRAY<T>>
          answer name clash     | state => DESCRIPTOR(case_file), choice => DESCRIPTOR(q_choice)                      | the input already contains the column(s) [q_choice_answer]
          bad on_error          | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(q_noul), on_error => 'IGNORE'    | `on_error => 'IGNORE'`: must be 'FAIL' or 'NULL'
          empty model           | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(q_noul), model => ''             | the model id must not be empty
          on_time omitted       | state => DESCRIPTOR(case_file), noul => DESCRIPTOR(q_noul)                          | the input has the event-time attribute 'submitted_at', so `on_time => DESCRIPTOR(submitted_at)` must be supplied
          """)
  void rejectsInvalidCalls(String name, String args, String message) {
    var timeArg = args.contains("submitted_at") ? "" : ", on_time => DESCRIPTOR(submitted_at)";
    var call = "input => TABLE apps, " + args + (name.equals("on_time omitted") ? "" : timeArg);
    assertThatThrownBy(() -> schema(call))
        .isInstanceOf(ValidationException.class)
        .satisfies(e -> assertThat(rootMessages(e)).contains(message));
  }

  @Test
  void rejectsNonLiteralModel() {
    assertThatThrownBy(
            () ->
                schema(
                    """
                    input => TABLE clean_input,
                    state => DESCRIPTOR(case_file),
                    noul => DESCRIPTOR(q_noul),
                    model => CONCAT('jev-', '1'),
                    on_time => DESCRIPTOR(submitted_at)"""))
        .satisfies(e -> assertThat(rootMessages(e)).contains("must be a string literal"));
  }

  @Test
  void rejectsReservedRowtimeColumn() {
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW with_rowtime AS "
            + "SELECT case_file, q_noul, id AS rowtime FROM no_time_input");
    assertThatThrownBy(
            () ->
                schema(
                    """
                    input => TABLE with_rowtime,
                    state => DESCRIPTOR(case_file),
                    noul => DESCRIPTOR(q_noul)"""))
        .satisfies(e -> assertThat(rootMessages(e)).contains("column 'rowtime' is reserved"));
  }

  @Test
  void rejectsUpdatingInput() {
    tEnv.executeSql(
        """
        CREATE TEMPORARY VIEW updating AS
        SELECT id, LAST_VALUE(case_file) AS case_file, LAST_VALUE(q_noul) AS q_noul
        FROM no_time_input GROUP BY id""");
    assertThatThrownBy(
            () ->
                tEnv.explainSql(
                    """
                    SELECT * FROM TypeSafeClassify(
                      input => TABLE updating,
                      state => DESCRIPTOR(case_file),
                      noul => DESCRIPTOR(q_noul))"""))
        .satisfies(e -> assertThat(rootMessages(e)).containsIgnoringCase("update"));
  }

  private static String rootMessages(Throwable t) {
    return ExceptionUtils.stringifyException(t);
  }
}
