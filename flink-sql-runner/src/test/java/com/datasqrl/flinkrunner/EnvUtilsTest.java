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
package com.datasqrl.flinkrunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;

class EnvUtilsTest {

  @Test
  void givenSystemEnv_whenGetEnvWithDefaults_thenReturnsEnvWithDefaults() {
    var env = EnvUtils.getEnvWithDefaults();

    assertThat(env).isNotNull();
    assertThat(env).containsKey("DEPLOYMENT_ID");
    assertThat(env).containsKey("DEPLOYMENT_TIMESTAMP");
  }

  @Test
  void givenSystemEnv_whenGetEnvWithDefaults_thenDeploymentIdIsValidUuid() {
    var env = EnvUtils.getEnvWithDefaults();

    String deploymentId = env.get("DEPLOYMENT_ID");
    assertThat(deploymentId)
        .isNotNull()
        .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  @Test
  void givenSystemEnv_whenGetEnvWithDefaults_thenDeploymentTimestampIsValidLong() {
    var env = EnvUtils.getEnvWithDefaults();

    String timestamp = env.get("DEPLOYMENT_TIMESTAMP");
    assertThat(timestamp).isNotNull();
    assertThat(Long.parseLong(timestamp)).isGreaterThan(0);
  }

  @Test
  void givenSystemEnv_whenGetEnvWithDefaults_thenReturnsImmutableMap() {
    var env = EnvUtils.getEnvWithDefaults();

    assertThatThrownBy(() -> env.put("NEW_KEY", "value"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void givenSystemEnv_whenGetEnvWithDefaults_thenIncludesSystemEnvironmentVariables() {
    var env = EnvUtils.getEnvWithDefaults();
    var systemEnv = System.getenv();

    // Verify that all system environment variables are present
    for (Map.Entry<String, String> entry : systemEnv.entrySet()) {
      assertThat(env).containsEntry(entry.getKey(), entry.getValue());
    }
  }

  @Test
  void givenMultipleCalls_whenGetEnvWithDefaults_thenGeneratesDifferentDeploymentIds() {
    var env1 = EnvUtils.getEnvWithDefaults();
    var env2 = EnvUtils.getEnvWithDefaults();

    assertThat(env1.get("DEPLOYMENT_ID")).isNotEqualTo(env2.get("DEPLOYMENT_ID"));
  }

  @Test
  void givenMultipleCalls_whenGetEnvWithDefaults_thenGeneratesDifferentTimestamps()
      throws InterruptedException {
    var env1 = EnvUtils.getEnvWithDefaults();
    Thread.sleep(2); // Small delay to ensure different timestamps
    var env2 = EnvUtils.getEnvWithDefaults();

    long timestamp1 = Long.parseLong(env1.get("DEPLOYMENT_TIMESTAMP"));
    long timestamp2 = Long.parseLong(env2.get("DEPLOYMENT_TIMESTAMP"));
    assertThat(timestamp2).isGreaterThanOrEqualTo(timestamp1);
  }

  @Test
  void givenConstructor_whenInvoked_thenThrowsUnsupportedOperationException() {
    assertThatThrownBy(
            () -> {
              var constructor = EnvUtils.class.getDeclaredConstructor();
              constructor.setAccessible(true);
              constructor.newInstance();
            })
        .hasCauseInstanceOf(UnsupportedOperationException.class);
  }
}
