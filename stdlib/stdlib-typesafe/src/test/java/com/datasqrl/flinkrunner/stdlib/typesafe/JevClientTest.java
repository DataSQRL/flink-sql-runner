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
import java.net.ServerSocket;
import java.time.Duration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JevClientTest {

  private MockJevServer server;
  private ClassifyMetrics metrics;
  private JevClient client;

  @BeforeEach
  void setUp() throws Exception {
    server = new MockJevServer();
    metrics = ClassifyMetrics.unregistered();
    client = new JevClient(config(server.endpoint()), metrics);
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  private static TypeSafeConfig config(String endpoint) {
    return TypeSafeConfig.builder()
        .apiKey("k")
        .endpoint(endpoint)
        .maxRetries(3)
        .maxBackoff(Duration.ofMillis(2))
        .connectTimeout(Duration.ofSeconds(2))
        .build();
  }

  private static JsonNode request() {
    var request = JsonSerializer.NODES.objectNode();
    request.put("model", "jev-1.13.0");
    request.putObject("state").put("x", 1);
    request.putObject("questions");
    return request;
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 500, 503, 529})
  void retriesTransientStatuses(int status) {
    server.enqueue(json(status, ""), json(status, ""));

    var response = client.classify(request());

    assertThat(response.path("model").asText()).isEqualTo("jev-1.13.0");
    assertThat(server.requests()).hasSize(3);
    assertThat(metrics.retries.getCount()).isEqualTo(2);
    assertThat(metrics.requests.getCount()).isEqualTo(1);
    assertThat(metrics.requestLatency.getCount()).isEqualTo(3);
  }

  @Test
  void givesUpAfterMaxRetries() {
    server.fallback(body -> json(503, "{\"error\":{\"type\":\"overloaded\"}}"));

    assertThatThrownBy(() -> client.classify(request()))
        .isInstanceOfSatisfying(
            ClassifyException.class,
            e -> {
              assertThat(e.getErrorClass()).isEqualTo(ErrorClass.RETRYABLE_EXHAUSTED);
              assertThat(e.getMessage())
                  .isEqualTo("HTTP_503: retries exhausted after 4 attempts (overloaded)");
            });
    assertThat(server.requests()).hasSize(4);
  }

  @Test
  void doesNotRetryValidationErrors() {
    server.fallback(body -> json(422, "{\"error\":{\"code\":\"has spaces so not a code\"}}"));

    assertThatThrownBy(() -> client.classify(request()))
        .isInstanceOfSatisfying(
            ClassifyException.class,
            e -> {
              assertThat(e.getErrorClass()).isEqualTo(ErrorClass.HTTP_4XX);
              assertThat(e.getMessage()).isEqualTo("HTTP_422: request rejected by the API");
            });
    assertThat(server.requests()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403})
  void authenticationErrorsAreFatal(int status) {
    server.fallback(body -> json(status, ""));

    assertThatThrownBy(() -> client.classify(request()))
        .isInstanceOfSatisfying(ClassifyException.class, e -> assertThat(e.isFatal()).isTrue());
    assertThat(server.requests()).hasSize(1);
  }

  @Test
  void invalidJsonIsAContractViolation() {
    server.fallback(body -> json(200, "not json"));
    assertThatThrownBy(() -> client.classify(request()))
        .isInstanceOfSatisfying(
            ClassifyException.class,
            e -> assertThat(e.getErrorClass()).isEqualTo(ErrorClass.CONTRACT));
  }

  @Test
  void retriesIoErrors() throws Exception {
    int unusedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      unusedPort = socket.getLocalPort();
    }
    var unreachable =
        new JevClient(config("http://127.0.0.1:" + unusedPort + "/v1/systemone"), metrics);

    assertThatThrownBy(() -> unreachable.classify(request()))
        .isInstanceOfSatisfying(
            ClassifyException.class,
            e -> {
              assertThat(e.getErrorClass()).isEqualTo(ErrorClass.RETRYABLE_EXHAUSTED);
              assertThat(e.getCode()).isEqualTo("IO_ERROR");
            });
    assertThat(metrics.retries.getCount()).isEqualTo(3);
  }

  @Test
  void sendsCredentialsAndJson() {
    client.classify(request());
    var recorded = server.requests().get(0);
    assertThat(recorded.headers().get("Authorization")).containsExactly("Bearer k");
    assertThat(recorded.headers().get("Content-type")).containsExactly("application/json");
    assertThat(recorded.body().path("state").path("x").asInt()).isEqualTo(1);
  }

  @Test
  void backoffIsCappedAndJittered() {
    var cap = Duration.ofSeconds(30);
    for (int attempt = 0; attempt < 50; attempt++) {
      var backoff = JevClient.backoff(attempt, cap);
      assertThat(backoff).isBetween(Duration.ZERO, cap);
      assertThat(backoff.toMillis())
          .isLessThanOrEqualTo(JevClient.INITIAL_BACKOFF.toMillis() << Math.min(attempt, 20));
    }
    assertThat(JevClient.backoff(10, Duration.ofMillis(5)))
        .isLessThanOrEqualTo(Duration.ofMillis(5));
  }
}
