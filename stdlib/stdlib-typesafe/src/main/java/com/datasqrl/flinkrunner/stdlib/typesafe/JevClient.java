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

import static com.datasqrl.flinkrunner.stdlib.typesafe.ClassifyException.ErrorClass.FATAL;
import static com.datasqrl.flinkrunner.stdlib.typesafe.ClassifyException.ErrorClass.HTTP_4XX;
import static com.datasqrl.flinkrunner.stdlib.typesafe.ClassifyException.ErrorClass.RETRYABLE_EXHAUSTED;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Synchronous client for the Jev endpoint, with retries for transient failures.
 *
 * <p>Request and response bodies are never logged or included in exception messages.
 */
@Slf4j
class JevClient {

  static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

  static final Duration INITIAL_BACKOFF = Duration.ofMillis(500);
  private static final Pattern MACHINE_CODE = Pattern.compile("[A-Za-z0-9_.\\-]{1,64}");

  private final TypeSafeConfig config;
  private final ClassifyMetrics metrics;
  private final HttpClient http;
  private final URI endpoint;

  JevClient(TypeSafeConfig config, ClassifyMetrics metrics) {
    this(
        config,
        metrics,
        HttpClient.newBuilder()
            .connectTimeout(config.connectTimeout())
            .version(HttpClient.Version.HTTP_1_1)
            .build());
  }

  JevClient(TypeSafeConfig config, ClassifyMetrics metrics, HttpClient http) {
    this.config = config;
    this.metrics = metrics;
    this.http = http;
    this.endpoint = URI.create(config.endpoint());
  }

  /**
   * Sends one classification request, retrying on 429, 529, 5xx and I/O errors.
   *
   * @throws ClassifyException on non-retryable or exhausted failures; {@link
   *     ClassifyException.ErrorClass#FATAL} for authentication errors
   */
  JsonNode classify(JsonNode request) {
    byte[] body;
    try {
      body = MAPPER.writeValueAsBytes(request);
    } catch (JsonProcessingException e) {
      throw ClassifyException.contract("request could not be serialized");
    }

    var httpRequest =
        HttpRequest.newBuilder(endpoint)
            .timeout(config.requestTimeout())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("Authorization", "Bearer " + config.apiKey())
            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
            .build();

    metrics.requests.inc();
    for (int attempt = 0; ; attempt++) {
      var canRetry = attempt < config.maxRetries();
      HttpResponse<byte[]> response;
      var start = System.nanoTime();
      try {
        response = http.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
      } catch (IOException e) {
        if (canRetry) {
          retry(attempt, "I/O error (" + e.getClass().getSimpleName() + ")");
          continue;
        }
        throw new ClassifyException(
            RETRYABLE_EXHAUSTED,
            "IO_ERROR",
            "%s after %d attempts".formatted(e.getClass().getSimpleName(), attempt + 1));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while calling the TypeSafe API", e);
      } finally {
        metrics.requestLatency.update(Duration.ofNanos(System.nanoTime() - start).toMillis());
      }

      var status = response.statusCode();
      if (status >= 200 && status < 300) {
        try {
          return MAPPER.readTree(response.body());
        } catch (IOException e) {
          throw ClassifyException.contract("response is not valid JSON");
        }
      }

      if (status == 401 || status == 403) {
        throw new ClassifyException(
            FATAL,
            "HTTP_" + status,
            "the TypeSafe API rejected the credentials. Check the configured API key.");
      }

      if (isRetryable(status)) {
        if (canRetry) {
          retry(attempt, "HTTP " + status);
          continue;
        }
        throw new ClassifyException(
            RETRYABLE_EXHAUSTED,
            "HTTP_" + status,
            describe(response, "retries exhausted after " + (attempt + 1) + " attempts"));
      }

      throw new ClassifyException(
          HTTP_4XX, "HTTP_" + status, describe(response, "request rejected by the API"));
    }
  }

  static boolean isRetryable(int status) {
    return status == 429 || status == 529 || (status >= 500 && status < 600);
  }

  private void retry(int attempt, String reason) {
    metrics.retries.inc();
    var backoff = backoff(attempt, config.maxBackoff());
    log.debug("Retrying TypeSafe request after {} in {} ms", reason, backoff.toMillis());
    try {
      Thread.sleep(backoff.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while backing off", e);
    }
  }

  /** Exponential backoff with full jitter, capped at {@code maxBackoff}. */
  static Duration backoff(int attempt, Duration maxBackoff) {
    var capMillis = maxBackoff.toMillis();
    var expMillis = INITIAL_BACKOFF.toMillis() << Math.min(attempt, 20);
    var bound = Math.max(1, Math.min(capMillis, expMillis));

    return Duration.ofMillis(ThreadLocalRandom.current().nextLong(bound + 1));
  }

  /**
   * Builds an error message from a fixed description plus a machine-readable error code from the
   * response, if any. Free-text messages are never copied, since they might echo request content.
   */
  private static String describe(HttpResponse<byte[]> response, String description) {
    try {
      var error = MAPPER.readTree(response.body()).path("error");
      for (String field : new String[] {"code", "type"}) {
        var code = error.path(field).asText("");
        if (MACHINE_CODE.matcher(code).matches()) {
          return description + " (" + code + ")";
        }
      }
    } catch (IOException | RuntimeException e) {
      // Not JSON, or not in the expected shape: fall back to the plain description.
    }
    return description;
  }

  static String toJsonString(JsonNode node) {
    try {
      return new String(MAPPER.writeValueAsBytes(node), StandardCharsets.UTF_8);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
