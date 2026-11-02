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

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.cfg.JsonNodeFeature;

/** A minimal in-process stand-in for the Jev endpoint. */
class MockJevServer implements AutoCloseable {

  record Response(int status, String body) {}

  record Recorded(JsonNode body, Map<String, List<String>> headers) {}

  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .configure(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES, false);

  private final HttpServer server;
  private final ConcurrentLinkedQueue<Response> scripted = new ConcurrentLinkedQueue<>();
  private final List<Recorded> requests = new CopyOnWriteArrayList<>();
  private volatile Function<JsonNode, Response> fallback = MockJevServer::answerAll;

  MockJevServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/v1/systemone",
        exchange -> {
          var body = MAPPER.readTree(exchange.getRequestBody());
          requests.add(new Recorded(body, Map.copyOf(exchange.getRequestHeaders())));
          var scriptedResponse = scripted.poll();
          var response = scriptedResponse != null ? scriptedResponse : fallback.apply(body);
          var bytes = response.body().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(response.status(), bytes.length == 0 ? -1 : bytes.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
    server.start();
  }

  String endpoint() {
    return "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/systemone";
  }

  /** Queues responses that are returned, in order, before falling back to the default. */
  MockJevServer enqueue(Response... responses) {
    scripted.addAll(List.of(responses));
    return this;
  }

  MockJevServer fallback(Function<JsonNode, Response> fallback) {
    this.fallback = fallback;
    return this;
  }

  List<Recorded> requests() {
    return new ArrayList<>(requests);
  }

  @Override
  public void close() {
    server.stop(0);
  }

  static Response json(int status, String body) {
    return new Response(status, body);
  }

  /**
   * Answers every question deterministically: Noul 0.91, Choice the first option, Score level 1.
   */
  static Response answerAll(JsonNode request) {
    var response = MAPPER.createObjectNode();
    response.put("model", request.path("model").asText().replace("latest", "1.13.0"));
    var answers = response.putObject("answers");
    var it = request.path("questions").fields();
    while (it.hasNext()) {
      var q = it.next();
      var type = q.getValue().path("type").asText();
      var answer = answers.putObject(q.getKey());
      answer.put("type", type);
      switch (type) {
        case "noul" -> answer.put("noul", 0.91);
        case "choice" -> {
          List<String> options = new ArrayList<>();
          q.getValue().path("criteria").fieldNames().forEachRemaining(options::add);
          answer.put("choice", options.get(0));
          answer.put("confidence", 0.74);
          var probabilities = answer.putObject("probabilities");
          for (int i = 0; i < options.size(); i++) {
            probabilities.put(options.get(i), i == 0 ? 0.81 : 0.19 / (options.size() - 1));
          }
        }
        case "score" -> {
          var levels = q.getValue().path("criteria").size();
          answer.put("score", 1.22);
          answer.put("confidence", 0.63);
          var probabilities = answer.putObject("probabilities");
          for (int i = 0; i < levels; i++) {
            probabilities.put(Integer.toString(i), i == 1 ? 0.64 : 0.36 / (levels - 1));
          }
        }
        default -> throw new IllegalArgumentException(type);
      }
    }
    response.putObject("usage").put("input_tokens", 1200).put("output_tokens", 30);
    return new Response(200, response.toString());
  }
}
