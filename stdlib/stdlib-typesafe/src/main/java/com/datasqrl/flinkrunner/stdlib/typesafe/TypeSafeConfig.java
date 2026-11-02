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

import java.time.Duration;
import java.util.function.UnaryOperator;
import lombok.Builder;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.util.TimeUtils;

/**
 * Runtime configuration, read from job parameters with environment fallback for the API key.
 *
 * <p>Components left unset in the builder fall back to their defaults.
 */
@Builder(toBuilder = true)
public record TypeSafeConfig(
    String apiKey,
    String endpoint,
    Duration connectTimeout,
    Duration requestTimeout,
    Integer maxRetries,
    Duration maxBackoff) {

  public static final String API_KEY = "typesafe.api-key";
  public static final String API_KEY_ENV = "TYPESAFE_API_KEY";
  public static final String ENDPOINT = "typesafe.endpoint";
  public static final String CONNECT_TIMEOUT = "typesafe.connect-timeout";
  public static final String REQUEST_TIMEOUT = "typesafe.request-timeout";
  public static final String MAX_RETRIES = "typesafe.max-retries";
  public static final String MAX_BACKOFF = "typesafe.max-backoff";

  public static final String DEFAULT_ENDPOINT = "https://api.typesafe.ai/v1/systemone";
  static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(120);
  static final int DEFAULT_MAX_RETRIES = 5;
  static final Duration DEFAULT_MAX_BACKOFF = Duration.ofSeconds(30);

  public TypeSafeConfig {
    endpoint = endpoint != null ? endpoint : DEFAULT_ENDPOINT;
    connectTimeout = connectTimeout != null ? connectTimeout : DEFAULT_CONNECT_TIMEOUT;
    requestTimeout = requestTimeout != null ? requestTimeout : DEFAULT_REQUEST_TIMEOUT;
    maxRetries = maxRetries != null ? maxRetries : DEFAULT_MAX_RETRIES;
    maxBackoff = maxBackoff != null ? maxBackoff : DEFAULT_MAX_BACKOFF;
  }

  static TypeSafeConfig from(FunctionContext context) {
    return from(key -> context.getJobParameter(key, null), System::getenv);
  }

  /**
   * Reads the configuration.
   *
   * @param jobParameters job parameter lookup, returning null for absent keys
   * @param env environment variable lookup, returning null for absent variables
   * @throws IllegalStateException if no API key is configured
   */
  static TypeSafeConfig from(UnaryOperator<String> jobParameters, UnaryOperator<String> env) {
    var apiKey = jobParameters.apply(API_KEY);
    if (apiKey == null || apiKey.isBlank()) {
      apiKey = env.apply(API_KEY_ENV);
    }

    if (apiKey == null || apiKey.isBlank()) {
      throw new IllegalStateException(
          "No TypeSafe API key configured. Set the job parameter '%s' or the environment variable '%s'."
              .formatted(API_KEY, API_KEY_ENV));
    }
    var builder = builder().apiKey(apiKey);
    var endpoint = jobParameters.apply(ENDPOINT);
    if (endpoint != null) {
      builder.endpoint(endpoint);
    }

    var connectTimeout = jobParameters.apply(CONNECT_TIMEOUT);
    if (connectTimeout != null) {
      builder.connectTimeout(TimeUtils.parseDuration(connectTimeout));
    }

    var requestTimeout = jobParameters.apply(REQUEST_TIMEOUT);
    if (requestTimeout != null) {
      builder.requestTimeout(TimeUtils.parseDuration(requestTimeout));
    }

    var maxRetries = jobParameters.apply(MAX_RETRIES);
    if (maxRetries != null) {
      builder.maxRetries(Integer.parseInt(maxRetries.trim()));
    }

    var maxBackoff = jobParameters.apply(MAX_BACKOFF);
    if (maxBackoff != null) {
      builder.maxBackoff(TimeUtils.parseDuration(maxBackoff));
    }

    return builder.build();
  }

  @Override
  public String toString() {
    // Lombok's @ToString doesn't support records, and the API key must never be printed.
    return "TypeSafeConfig(endpoint="
        + endpoint
        + ", connectTimeout="
        + connectTimeout
        + ", requestTimeout="
        + requestTimeout
        + ", maxRetries="
        + maxRetries
        + ", maxBackoff="
        + maxBackoff
        + ")";
  }
}
