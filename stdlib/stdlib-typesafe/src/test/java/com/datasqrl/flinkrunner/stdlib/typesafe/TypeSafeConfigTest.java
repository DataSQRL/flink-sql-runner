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

import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TypeSafeConfigTest {

  @Test
  void readsJobParameters() {
    var params =
        Map.of(
            TypeSafeConfig.API_KEY, "job-key",
            TypeSafeConfig.ENDPOINT, "http://localhost:1/v1/systemone",
            TypeSafeConfig.CONNECT_TIMEOUT, "3 s",
            TypeSafeConfig.REQUEST_TIMEOUT, "1 min",
            TypeSafeConfig.MAX_RETRIES, "7",
            TypeSafeConfig.MAX_BACKOFF, "250 ms");
    var config = TypeSafeConfig.from(params::get, env -> "env-key");

    assertThat(config.apiKey()).isEqualTo("job-key");
    assertThat(config.endpoint()).isEqualTo("http://localhost:1/v1/systemone");
    assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(3));
    assertThat(config.requestTimeout()).isEqualTo(Duration.ofMinutes(1));
    assertThat(config.maxRetries()).isEqualTo(7);
    assertThat(config.maxBackoff()).isEqualTo(Duration.ofMillis(250));
    assertThat(config.toString()).doesNotContain("job-key");
  }

  @Test
  void fallsBackToEnvironmentAndDefaults() {
    var config =
        TypeSafeConfig.from(
            key -> null, env -> env.equals(TypeSafeConfig.API_KEY_ENV) ? "env-key" : null);
    assertThat(config.apiKey()).isEqualTo("env-key");
    assertThat(config.endpoint()).isEqualTo(TypeSafeConfig.DEFAULT_ENDPOINT);
    assertThat(config.maxRetries()).isEqualTo(TypeSafeConfig.DEFAULT_MAX_RETRIES);
  }

  @Test
  void failsWithoutApiKey() {
    assertThatThrownBy(() -> TypeSafeConfig.from(key -> null, env -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(TypeSafeConfig.API_KEY)
        .hasMessageContaining(TypeSafeConfig.API_KEY_ENV);
  }
}
