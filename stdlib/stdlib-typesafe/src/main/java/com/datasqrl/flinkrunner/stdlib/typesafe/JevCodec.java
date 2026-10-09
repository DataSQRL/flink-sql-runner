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

import static com.datasqrl.flinkrunner.stdlib.typesafe.ClassifyException.contract;
import static com.datasqrl.flinkrunner.stdlib.typesafe.JsonSerializer.NODES;

import com.datasqrl.flinkrunner.stdlib.typesafe.ClassifyPlan.Question;
import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;

/**
 * Builds Jev requests from input rows and maps responses back onto the typed answer columns.
 * Created once per task from a {@link ClassifyPlan}.
 */
class JevCodec {

  /** A request together with the questions it contains. */
  record Request(ObjectNode body, boolean[] included, int[] scoreLevels) {}

  /** Answers mapped from a response. */
  record Answers(Object[] values, String model, long inputTokens, long outputTokens) {}

  static final int MIN_SCORE_LEVELS = 2;
  static final int MAX_SCORE_LEVELS = 10;

  private final ClassifyPlan plan;
  private final JsonSerializer state;
  private final JsonSerializer[] instructions;
  private final JsonSerializer[] criteria;

  JevCodec(ClassifyPlan plan) {
    this.plan = plan;
    this.state = JsonSerializer.forType(plan.getStateType(), "state");
    var questions = plan.getQuestions();
    this.instructions = new JsonSerializer[questions.size()];
    this.criteria = new JsonSerializer[questions.size()];
    for (int i = 0; i < questions.size(); i++) {
      var q = questions.get(i);
      instructions[i] = JsonSerializer.forType(q.instructionsType(), q.column() + ".instructions");
      criteria[i] = JsonSerializer.forType(q.criteriaType(), q.column() + ".criteria");
    }
  }

  /**
   * Builds the request for a row with a non-null state.
   *
   * @return the request, or null if every question of the row is NULL
   * @throws ClassifyException on contract violations of the input
   */
  Request buildRequest(Row input) {
    var questions = plan.getQuestions();
    var included = new boolean[questions.size()];
    var scoreLevels = new int[questions.size()];
    var questionsNode = NODES.objectNode();

    for (int i = 0; i < questions.size(); i++) {
      var q = questions.get(i);
      var value = (Row) input.getField(q.columnIndex());
      if (value == null) {
        continue;
      }

      var instructionsValue = value.getField(q.instructionsField());
      if (instructionsValue == null) {
        throw contract("question '" + q.column() + "' has NULL instructions");
      }

      var criteriaValue = value.getField(q.criteriaField());
      var node = NODES.objectNode();
      node.put("type", q.kind().getArgName());
      node.set("instructions", instructions[i].toJson(instructionsValue));

      switch (q.kind()) {
        case NOUL:
          if (criteriaValue != null) {
            node.set("criteria", criteria[i].toJson(criteriaValue));
          }
          break;
        case CHOICE:
          // The options are part of the type, so a NULL criteria value still defines them.
          node.set(
              "criteria",
              criteriaValue != null
                  ? criteria[i].toJson(criteriaValue)
                  : criteria[i].toJson(Row.of(new Object[q.options().size()])));
          break;
        case SCORE:
          if (criteriaValue == null) {
            throw contract("score question '" + q.column() + "' has NULL criteria");
          }
          var levels = Array.getLength(criteriaValue);
          if (levels < MIN_SCORE_LEVELS || levels > MAX_SCORE_LEVELS) {
            throw contract(
                "score question '%s' has %d levels, but %d to %d are required"
                    .formatted(q.column(), levels, MIN_SCORE_LEVELS, MAX_SCORE_LEVELS));
          }
          scoreLevels[i] = levels;
          node.set("criteria", criteria[i].toJson(criteriaValue));
          break;
        default:
          throw new IllegalStateException();
      }
      questionsNode.set(q.column(), node);
      included[i] = true;
    }

    if (questionsNode.isEmpty()) {
      return null;
    }

    var body = NODES.objectNode();
    body.put("model", plan.getModel());
    body.set("state", state.toJson(input.getField(plan.getStateIndex())));
    body.set("questions", questionsNode);

    return new Request(body, included, scoreLevels);
  }

  /**
   * Maps a response onto the answer columns.
   *
   * @throws ClassifyException if the response violates the API contract
   */
  Answers mapResponse(Request request, JsonNode response) {
    if (!response.isObject()) {
      throw contract("response is not a JSON object");
    }

    var model = response.path("model").asText("");
    if (model.isEmpty()) {
      throw contract("response has no 'model'");
    }

    var answers = response.path("answers");
    if (!answers.isObject()) {
      throw contract("response has no 'answers' object");
    }

    var questions = plan.getQuestions();
    var values = new Object[questions.size()];
    var expected = new HashSet<String>();
    for (int i = 0; i < questions.size(); i++) {
      if (!request.included()[i]) {
        continue;
      }

      var q = questions.get(i);
      expected.add(q.column());
      var answer = answers.get(q.column());
      if (answer == null || !answer.isObject()) {
        throw contract("response has no answer for question '" + q.column() + "'");
      }

      var type = answer.path("type").asText("");
      if (!type.equals(q.kind().getArgName())) {
        throw contract(
            "answer for question '%s' has type '%s', but '%s' was asked"
                .formatted(q.column(), type, q.kind().getArgName()));
      }

      values[i] =
          switch (q.kind()) {
            case NOUL -> number(answer, "noul", q);
            case CHOICE -> choice(answer, q);
            case SCORE -> score(answer, q, request.scoreLevels()[i]);
          };
    }

    var names = answers.fieldNames();
    while (names.hasNext()) {
      var name = names.next();
      if (!expected.contains(name)) {
        throw contract("response has an answer for unknown question '" + name + "'");
      }
    }

    var usage = response.path("usage");

    return new Answers(
        values, model, usage.path("input_tokens").asLong(0), usage.path("output_tokens").asLong(0));
  }

  private static Row choice(JsonNode answer, Question q) {
    var choice = answer.path("choice").asText(null);
    if (choice == null || !q.options().contains(choice)) {
      throw contract(
          "answer for question '%s' has a 'choice' that is not one of %s"
              .formatted(q.column(), q.options()));
    }

    var probabilities = probabilities(answer, q, q.options());
    var values = new Object[q.options().size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = probabilities.get(q.options().get(i)).doubleValue();
    }

    return Row.of(choice, number(answer, "confidence", q), Row.of(values));
  }

  private static Row score(JsonNode answer, Question q, int levels) {
    var keys = IntStream.range(0, levels).mapToObj(Integer::toString).toList();
    var probabilities = probabilities(answer, q, keys);
    var values = new Double[levels];
    for (int i = 0; i < levels; i++) {
      values[i] = probabilities.get(keys.get(i)).doubleValue();
    }

    return Row.of(number(answer, "score", q), number(answer, "confidence", q), values);
  }

  /** Returns the probabilities object after checking its keys match {@code keys} exactly. */
  private static JsonNode probabilities(JsonNode answer, Question q, List<String> keys) {
    var probabilities = answer.path("probabilities");
    if (!probabilities.isObject()) {
      throw contract("answer for question '" + q.column() + "' has no 'probabilities'");
    }

    Set<String> actual = new HashSet<>();
    probabilities.fieldNames().forEachRemaining(actual::add);
    if (!actual.equals(new HashSet<>(keys))) {
      throw contract(
          "answer for question '%s' has probabilities for %s, but %s were expected"
              .formatted(q.column(), actual, keys));
    }

    for (var key : keys) {
      if (!probabilities.get(key).isNumber()) {
        throw contract(
            "answer for question '%s' has a non-numeric probability for '%s'"
                .formatted(q.column(), key));
      }
    }

    return probabilities;
  }

  private static Double number(JsonNode answer, String field, Question q) {
    var node = answer.path(field);
    if (node.isNumber()) {
      return node.doubleValue();
    }

    throw contract("answer for question '%s' has no numeric '%s'".formatted(q.column(), field));
  }
}
