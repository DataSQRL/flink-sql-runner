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

import java.math.BigDecimal;
import java.util.function.Consumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JevCodecTest {

  private final JevCodec codec = new JevCodec(TestPlans.plan(ErrorPolicy.NULL));

  static Row input(Row noul, Row choice, Row score) {
    return Row.of("id-1", Row.of(new BigDecimal("12.50"), "secret notes"), noul, choice, score);
  }

  static Row noul() {
    return Row.of("Verified?", Row.of("yes", "no"));
  }

  static Row choice() {
    return Row.of("Which?", Row.of("A desc", null));
  }

  static Row score() {
    return Row.of("How much?", new String[] {"low", "mid", "high"});
  }

  @Test
  void buildsRequestsAndMapsAllKinds() throws Exception {
    var request = codec.buildRequest(input(noul(), choice(), score()));

    assertThat(JevClient.toJsonString(request.body()))
        .isEqualTo(
            "{\"model\":\"jev-1.13.0\","
                + "\"state\":{\"amount\":12.50,\"notes\":\"secret notes\"},"
                + "\"questions\":{"
                + "\"q_noul\":{\"type\":\"noul\",\"instructions\":\"Verified?\","
                + "\"criteria\":{\"true\":\"yes\",\"false\":\"no\"}},"
                + "\"q_choice\":{\"type\":\"choice\",\"instructions\":\"Which?\","
                + "\"criteria\":{\"a\":\"A desc\",\"b\":null}},"
                + "\"q_score\":{\"type\":\"score\",\"instructions\":\"How much?\","
                + "\"criteria\":[\"low\",\"mid\",\"high\"]}}}");

    var answers = codec.mapResponse(request, JevClient.MAPPER.readTree(answerAll(request)));

    assertThat(answers.model()).isEqualTo("jev-1.13.0");
    assertThat(answers.inputTokens()).isEqualTo(1200);
    assertThat(answers.outputTokens()).isEqualTo(30);
    assertThat(answers.values()[0]).isEqualTo(0.91);
    assertThat(answers.values()[1]).isEqualTo(Row.of("a", 0.74, Row.of(0.81, 0.19)));
    var score = (Row) answers.values()[2];
    assertThat(score.getField(0)).isEqualTo(1.22);
    assertThat((Double[]) score.getField(2)).containsExactly(0.18, 0.64, 0.18);
  }

  private static String answerAll(JevCodec.Request request) {
    return MockJevServer.answerAll(request.body()).body();
  }

  @Test
  void omitsNullQuestionsAndNullNoulCriteria() {
    var request = codec.buildRequest(input(Row.of("Verified?", null), null, score()));
    var questions = request.body().path("questions");
    assertThat(questions.has("q_choice")).isFalse();
    assertThat(questions.path("q_noul").has("criteria")).isFalse();
    assertThat(request.included()).containsExactly(true, false, true);
  }

  @Test
  void choiceOptionsComeFromTheTypeWhenCriteriaAreNull() {
    var request = codec.buildRequest(input(null, Row.of("Which?", null), null));
    assertThat(JevClient.toJsonString(request.body().path("questions").path("q_choice")))
        .contains("\"criteria\":{\"a\":null,\"b\":null}");
  }

  @Test
  void rowsWithoutQuestionsNeedNoRequest() {
    assertThat(codec.buildRequest(input(null, null, null))).isNull();
  }

  @Test
  void nullInstructionsAreAContractViolation() {
    assertThatThrownBy(() -> codec.buildRequest(input(Row.of(null, null), null, null)))
        .isInstanceOf(ClassifyException.class)
        .hasMessage("CONTRACT: question 'q_noul' has NULL instructions");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 11})
  void scoreLevelCountIsCheckedAtRuntime(int levels) {
    var score = Row.of("How much?", new String[levels]);
    assertThatThrownBy(() -> codec.buildRequest(input(null, null, score)))
        .isInstanceOf(ClassifyException.class)
        .hasMessageContaining("has " + levels + " levels, but 2 to 10 are required");
  }

  @Test
  void nullScoreCriteriaAreAContractViolation() {
    assertThatThrownBy(() -> codec.buildRequest(input(null, null, Row.of("How?", null))))
        .isInstanceOf(ClassifyException.class)
        .hasMessageContaining("has NULL criteria");
  }

  private void assertViolation(Consumer<ObjectNode> corrupt, String message) throws Exception {
    var request = codec.buildRequest(input(noul(), choice(), score()));
    var response = (ObjectNode) JevClient.MAPPER.readTree(answerAll(request));
    corrupt.accept(response);
    assertThatThrownBy(() -> codec.mapResponse(request, response))
        .isInstanceOfSatisfying(
            ClassifyException.class,
            e -> {
              assertThat(e.getErrorClass()).isEqualTo(ClassifyException.ErrorClass.CONTRACT);
              assertThat(e.getMessage()).contains(message);
            });
  }

  private static ObjectNode answer(ObjectNode response, String q) {
    return (ObjectNode) response.path("answers").path(q);
  }

  @Test
  void detectsContractViolations() throws Exception {
    assertViolation(r -> r.remove("model"), "response has no 'model'");
    assertViolation(r -> r.remove("answers"), "response has no 'answers' object");
    assertViolation(
        r -> ((ObjectNode) r.path("answers")).remove("q_score"),
        "response has no answer for question 'q_score'");
    assertViolation(
        r -> ((ObjectNode) r.path("answers")).putObject("q_other").put("type", "noul"),
        "unknown question 'q_other'");
    assertViolation(
        r -> answer(r, "q_noul").put("type", "score"),
        "answer for question 'q_noul' has type 'score', but 'noul' was asked");
    assertViolation(r -> answer(r, "q_noul").put("noul", "high"), "has no numeric 'noul'");
    assertViolation(
        r -> answer(r, "q_choice").put("choice", "c"), "has a 'choice' that is not one of [a, b]");
    assertViolation(
        r -> ((ObjectNode) answer(r, "q_choice").path("probabilities")).remove("b"),
        "has probabilities for [a], but [a, b] were expected");
    assertViolation(
        r -> ((ObjectNode) answer(r, "q_choice").path("probabilities")).put("c", 0.0),
        "but [a, b] were expected");
    assertViolation(
        r -> ((ObjectNode) answer(r, "q_score").path("probabilities")).remove("2"),
        "but [0, 1, 2] were expected");
    assertViolation(
        r -> ((ObjectNode) answer(r, "q_score").path("probabilities")).put("1", "x"),
        "non-numeric probability for '1'");
    assertViolation(r -> answer(r, "q_score").remove("probabilities"), "has no 'probabilities'");
    assertViolation(r -> answer(r, "q_score").remove("confidence"), "no numeric 'confidence'");
  }

  @Test
  void rejectsNonObjectResponses() {
    var request = codec.buildRequest(input(noul(), null, null));
    assertThatThrownBy(() -> codec.mapResponse(request, JevClient.MAPPER.createArrayNode()))
        .hasMessageContaining("not a JSON object");
  }
}
