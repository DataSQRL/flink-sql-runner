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

import com.datasqrl.flinkrunner.stdlib.typesafe.ClassifyException.ErrorClass;
import java.time.Duration;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Error policies and NULL semantics of {@link typesafe_classify#classify}. */
class TypeSafeClassifyTest {

  private MockJevServer server;
  private ClassifyMetrics metrics;

  @BeforeEach
  void setUp() throws Exception {
    server = new MockJevServer();
    metrics = ClassifyMetrics.unregistered();
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  private typesafe_classify open(ErrorPolicy policy) {
    var function = new typesafe_classify(TestPlans.plan(policy));
    function.open(
        TypeSafeConfig.builder()
            .apiKey("k")
            .endpoint(server.endpoint())
            .maxRetries(1)
            .maxBackoff(Duration.ofMillis(1))
            .build(),
        metrics);
    return function;
  }

  private static Row row() {
    return JevCodecTest.input(JevCodecTest.noul(), JevCodecTest.choice(), JevCodecTest.score());
  }

  @Test
  void appendsAnswersModelAndError() {
    var result = open(ErrorPolicy.NULL).classify(row());
    assertThat(result.getArity()).isEqualTo(5);
    assertThat(result.getField(0)).isEqualTo(0.91);
    assertThat(result.getField(3)).isEqualTo("jev-1.13.0");
    assertThat(result.getField(4)).isNull();
    assertThat(metrics.inputTokens.getCount()).isEqualTo(1200);
    assertThat(metrics.outputTokens.getCount()).isEqualTo(30);
  }

  @Test
  void failPolicyHasNoErrorColumn() {
    assertThat(open(ErrorPolicy.FAIL).classify(row()).getArity()).isEqualTo(4);
  }

  @Test
  void nullStateSkipsTheApi() {
    var input = Row.of("id", null, JevCodecTest.noul(), null, null);
    var result = open(ErrorPolicy.FAIL).classify(input);
    assertThat(result).isEqualTo(Row.of(null, null, null, "jev-1.13.0"));
    assertThat(server.requests()).isEmpty();
  }

  @Test
  void nullPolicyEmitsNullAnswersWithError() {
    server.fallback(body -> json(503, ""));
    var result = open(ErrorPolicy.NULL).classify(row());
    assertThat(result)
        .isEqualTo(
            Row.of(null, null, null, "jev-1.13.0", "HTTP_503: retries exhausted after 2 attempts"));
    assertThat(metrics.nullRows.getCount()).isEqualTo(1);
    assertThat(metrics.errors(ErrorClass.RETRYABLE_EXHAUSTED).getCount()).isEqualTo(1);
  }

  @Test
  void contractViolationsFollowThePolicy() {
    server.fallback(body -> json(200, "{\"model\":\"jev-1.13.0\",\"answers\":{}}"));
    var result = open(ErrorPolicy.NULL).classify(row());
    assertThat((String) result.getField(4)).startsWith("CONTRACT: response has no answer");
    assertThat(metrics.errors(ErrorClass.CONTRACT).getCount()).isEqualTo(1);

    assertThatThrownBy(() -> open(ErrorPolicy.FAIL).classify(row()))
        .isInstanceOf(ClassifyException.class);
  }

  @Test
  void authenticationErrorsFailUnderBothPolicies() {
    server.fallback(body -> json(401, ""));
    assertThatThrownBy(() -> open(ErrorPolicy.NULL).classify(row()))
        .isInstanceOf(ClassifyException.class)
        .hasMessageStartingWith("HTTP_401");
    assertThatThrownBy(() -> open(ErrorPolicy.FAIL).classify(row()))
        .isInstanceOf(ClassifyException.class);
    assertThat(metrics.nullRows.getCount()).isZero();
  }

  @Test
  void requiresSpecialization() {
    assertThatThrownBy(
            () ->
                new typesafe_classify().open(TypeSafeConfig.builder().apiKey("k").build(), metrics))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void isNotDeterministic() {
    assertThat(new typesafe_classify().isDeterministic()).isFalse();
  }
}
