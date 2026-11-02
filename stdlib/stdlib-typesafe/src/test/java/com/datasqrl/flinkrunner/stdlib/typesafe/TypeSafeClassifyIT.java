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

import static com.datasqrl.flinkrunner.stdlib.typesafe.MockJevServer.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Runs the loan underwriting example end-to-end on a MiniCluster against a mock Jev server. */
@ExtendWith(MiniClusterExtension.class)
class TypeSafeClassifyIT {

  private MockJevServer server;
  private TableEnvironment tEnv;

  @BeforeEach
  void setUp() throws Exception {
    server = new MockJevServer();
    tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
    tEnv.getConfig()
        .set(
            PipelineOptions.GLOBAL_JOB_PARAMETERS,
            Map.of(
                TypeSafeConfig.API_KEY, "test-key",
                TypeSafeConfig.ENDPOINT, server.endpoint(),
                TypeSafeConfig.MAX_RETRIES, "2",
                TypeSafeConfig.MAX_BACKOFF, "5 ms"));
    tEnv.executeSql(
        "CREATE TEMPORARY FUNCTION TypeSafeClassify AS '"
            + typesafe_classify.class.getName()
            + "'");

    // Three applications. Application 2 has no purpose question, application 3 has no state.
    tEnv.executeSql(
        """
        CREATE TABLE loan_applications (
          n INT,
          submitted_at TIMESTAMP_LTZ(3),
          WATERMARK FOR submitted_at AS submitted_at - INTERVAL '10' SECOND
        ) WITH (
          'connector' = 'datagen',
          'fields.n.kind' = 'sequence',
          'fields.n.start' = '1',
          'fields.n.end' = '3'
        )""");
    tEnv.executeSql(
        """
        CREATE TEMPORARY VIEW underwriting_input AS
        SELECT
          CONCAT('app-', CAST(n AS STRING)) AS application_id,
          CAST(25000.00 AS DECIMAL(12, 2)) AS amount,
          submitted_at,
          CASE WHEN n < 3 THEN CAST(ROW(
            ROW(CAST(25000.00 AS DECIMAL(12, 2)), 48,
                'Consolidate two credit cards and fix the roof'),
            ROW('Cascade Logistics', 'Dispatcher', CAST(3.5 AS DECIMAL(4, 1)),
                CAST(68000.00 AS DECIMAL(12, 2)), 702, 'Employer confirmed title.'),
            CAST(0.412 AS DECIMAL(6, 3))
          ) AS ROW<
            loan          ROW<amount DECIMAL(12, 2), term_months INT, purpose STRING>,
            applicant     ROW<employer STRING, job_title STRING,
                              years_employed DECIMAL(4, 1),
                              stated_annual_income DECIMAL(12, 2),
                              credit_score INT, verification_notes STRING>,
            projected_dti DECIMAL(6, 3)
          >) END AS case_file,

          CAST(ROW(
            'Do `applicant.verification_notes` corroborate the application?',
            ROW('Notes confirm employer, role and income', 'Notes contradict the application')
          ) AS ROW<instructions STRING,
                   criteria ROW<`true` STRING, `false` STRING>>
          ) AS q_income_verified,

          CASE WHEN n <> 2 THEN CAST(ROW(
            'Which category best describes the purpose of the loan in `loan.purpose`?',
            ROW('Paying off existing credit cards or loans',
                'Repairs, renovation or improvements to a residence',
                'Purchase or repair of a vehicle',
                'Starting or funding a business',
                CAST(NULL AS STRING))
          ) AS ROW<instructions STRING,
                   criteria ROW<debt_consolidation STRING, home_improvement STRING,
                                vehicle STRING, business STRING, other STRING>>
          ) END AS q_purpose,

          CAST(ROW(
            'How much underwriting scrutiny does this application warrant?',
            ARRAY['No concerns', 'Minor concerns', 'Significant concerns', 'Serious red flags']
          ) AS ROW<instructions STRING, criteria ARRAY<STRING>>
          ) AS q_scrutiny
        FROM loan_applications""");
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  private String judgments(String onError) {
    return """
        TypeSafeClassify(
          input    => TABLE underwriting_input,
          state    => DESCRIPTOR(case_file),
          noul     => DESCRIPTOR(q_income_verified),
          choice   => DESCRIPTOR(q_purpose),
          score    => DESCRIPTOR(q_scrutiny),
          model    => 'jev-1.13.0',
          on_error => '%s',
          on_time  => DESCRIPTOR(submitted_at)
        )"""
        .formatted(onError);
  }

  private List<Row> collect(String query) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = tEnv.executeSql(query).collect()) {
      it.forEachRemaining(rows::add);
    }
    return rows;
  }

  @Test
  void classifiesLoanApplications() throws Exception {
    var rows =
        collect(
            "SELECT application_id, q_income_verified_answer, q_purpose_answer, "
                + "q_scrutiny_answer, _model, _error, rowtime = submitted_at AS same_time "
                + "FROM "
                + judgments("NULL"));

    assertThat(rows).hasSize(3);
    var first = rows.get(0);
    assertThat(first.getField(0)).isEqualTo("app-1");
    assertThat(first.getField(1)).isEqualTo(0.91);
    var purpose = (Row) first.getField(2);
    assertThat(purpose.getField(0)).isEqualTo("debt_consolidation");
    assertThat(purpose.getField(1)).isEqualTo(0.74);
    assertThat(((Row) purpose.getField(2)).getArity()).isEqualTo(5);
    assertThat(((Row) purpose.getField(2)).getField(0)).isEqualTo(0.81);
    var scrutiny = (Row) first.getField(3);
    assertThat(scrutiny.getField(0)).isEqualTo(1.22);
    assertThat((Double[]) scrutiny.getField(2)).hasSize(4).contains(0.64);
    assertThat(first.getField(4)).isEqualTo("jev-1.13.0");
    assertThat(first.getField(5)).isNull();
    assertThat(first.getField(6)).isEqualTo(true);

    // A NULL question is omitted from the request and answered with NULL.
    var second = rows.get(1);
    assertThat(second.getField(1)).isEqualTo(0.91);
    assertThat(second.getField(2)).isNull();
    assertThat(second.getField(3)).isNotNull();

    // A NULL state makes no API call and yields all-NULL answers.
    var third = rows.get(2);
    assertThat(third.getField(1)).isNull();
    assertThat(third.getField(2)).isNull();
    assertThat(third.getField(3)).isNull();
    assertThat(third.getField(4)).isEqualTo("jev-1.13.0");
    assertThat(third.getField(5)).isNull();

    var requests = server.requests();
    assertThat(requests).hasSize(2);
    assertThat(requests.get(0).headers().get("Authorization")).containsExactly("Bearer test-key");

    var request = requests.get(0).body();
    assertThat(request.path("model").asText()).isEqualTo("jev-1.13.0");
    assertThat(request.path("state").toString())
        .isEqualTo(
            "{\"loan\":{\"amount\":25000.00,\"term_months\":48,"
                + "\"purpose\":\"Consolidate two credit cards and fix the roof\"},"
                + "\"applicant\":{\"employer\":\"Cascade Logistics\",\"job_title\":\"Dispatcher\","
                + "\"years_employed\":3.5,\"stated_annual_income\":68000.00,\"credit_score\":702,"
                + "\"verification_notes\":\"Employer confirmed title.\"},"
                + "\"projected_dti\":0.412}");
    var questions = request.path("questions");
    assertThat(questions.path("q_income_verified").path("type").asText()).isEqualTo("noul");
    assertThat(questions.path("q_income_verified").path("criteria").has("true")).isTrue();
    assertThat(questions.path("q_purpose").path("type").asText()).isEqualTo("choice");
    assertThat(questions.path("q_purpose").path("criteria").path("other").isNull()).isTrue();
    assertThat(questions.path("q_scrutiny").path("criteria").size()).isEqualTo(4);
    assertThat(requests.get(1).body().path("questions").has("q_purpose")).isFalse();
  }

  @Test
  void feedsEventTimeWindows() throws Exception {
    tEnv.executeSql(
        "CREATE TEMPORARY VIEW underwriting_judgments AS SELECT * FROM " + judgments("NULL"));
    var rows =
        collect(
            """
            SELECT COUNT(*) AS applications, COUNT(_error) AS errors,
                   SUM(CASE WHEN q_purpose_answer.probabilities.business IS NULL
                            THEN 0 ELSE 1 END) AS with_purpose
            FROM TABLE(
              TUMBLE(TABLE underwriting_judgments, DESCRIPTOR(rowtime), INTERVAL '1' DAY))
            GROUP BY window_start, window_end""");
    assertThat(rows.stream().mapToLong(r -> (Long) r.getField(0)).sum()).isEqualTo(3);
  }

  @Test
  void nullPolicyEmitsErrorRows() throws Exception {
    server.enqueue(
        json(422, "{\"error\":{\"code\":\"token_limit_exceeded\",\"message\":\"secret state\"}}"));

    var rows =
        collect(
            "SELECT application_id, q_income_verified_answer, _model, _error FROM "
                + judgments("NULL"));

    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getField(1)).isNull();
    assertThat(rows.get(0).getField(2)).isEqualTo("jev-1.13.0");
    assertThat((String) rows.get(0).getField(3))
        .isEqualTo("HTTP_422: request rejected by the API (token_limit_exceeded)")
        .doesNotContain("secret");
    assertThat(rows.get(1).getField(1)).isEqualTo(0.91);
    assertThat(rows.get(1).getField(3)).isNull();
  }

  @Test
  void failPolicyFailsTheJob() {
    server.fallback(body -> json(422, "{}"));
    assertThatThrownBy(() -> collect("SELECT * FROM " + judgments("FAIL")))
        .satisfies(
            e ->
                assertThat(ExceptionUtils.findThrowable(e, ClassifyException.class))
                    .hasValueSatisfying(c -> assertThat(c.getMessage()).startsWith("HTTP_422")));
  }

  @Test
  void authenticationErrorsFailEvenUnderNullPolicy() {
    server.fallback(body -> json(401, "{}"));
    assertThatThrownBy(() -> collect("SELECT * FROM " + judgments("NULL")))
        .satisfies(
            e ->
                assertThat(ExceptionUtils.findThrowable(e, ClassifyException.class))
                    .hasValueSatisfying(c -> assertThat(c.getMessage()).startsWith("HTTP_401")));
  }
}
