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
package com.datasqrl.flinkrunner.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EnvVarResolverTest {

  private EnvVarResolver resolver;

  @ParameterizedTest
  @CsvSource({
    "'Hello, ${USER}!', 'Hello, John!'", // Positive case: USER variable exists, tail is appended
    "'Path: ${PATH}', 'Path: /usr/bin'", // Positive case: PATH variable exists
    "'No match here', 'No match here'", // Case with no placeholders
    "'Partial ${USER_HOME', 'Partial ${USER_HOME'" // Case to ensure partial match is not replaced
  })
  void givenEnvVariables_whenReplaceWithEnv_thenReplaceCorrectlyVars(
      String command, String expected) {
    Map<String, String> envVariables =
        Map.of(
            "USER", "John",
            "PATH", "/usr/bin");
    resolver = EnvVarResolver.of(envVariables);
    var result = resolver.resolve(command);
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "'Hello, ${USER}'", // USER variable missing
        "'Path: ${UNKNOWN}'", // UNKNOWN variable missing
        "'Combined ${VAR1} and ${VAR2}'" // Multiple missing variables
      })
  void givenMissingEnvVariables_whenResolveEnv_Vars_thenThrowException(String command) {
    Map<String, String> envVariables = Map.of();
    resolver = EnvVarResolver.of(envVariables); // Empty map to simulate missing variables
    assertThatThrownBy(() -> resolver.resolve(command))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "The following environment variables were referenced, but not found:");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "'${USER} is different from ${USER_NAME}'",
        "'${NAME} is different from ${USER_NAME}'"
      })
  void givenSimilarEnvVariableNames_whenResolveEnv_Vars_thenPartialMatchDoesNotOccur(
      String command) {
    Map<String, String> envVariables =
        Map.of(
            "USER", "John",
            "NAME", "exists");
    resolver = EnvVarResolver.of(envVariables);
    assertThatThrownBy(() -> resolver.resolve(command))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The following environment variables were referenced, but not found: [USER_NAME]");
  }

  @ParameterizedTest
  @CsvSource({
    // Env var not present, fallback used
    "'${USERNAME:-guest}', 'guest'",
    "'${USERNAME:=guest}', 'guest'",
    "'Welcome ${USERNAME:-anonymous}', 'Welcome anonymous'",
    // Env var present, fallback ignored
    "'${USER:-guest}', 'John'",
    "'${USER:=guest}', 'John'",
    "'Path: ${PATH:-/default}', 'Path: /usr/bin'",
    // Empty default
    "'Empty fallback: ${MISSING:-}', 'Empty fallback: '",
    // Mixed present and fallback
    "'User=${USER}, ID=${ID:-0000}', 'User=John, ID=0000'",
    "'${MISSING1:-default1} and ${MISSING2:=default2}', 'default1 and default2'"
  })
  void givenDefaultEnvValues_whenResolve_thenFallbackOrUseEnvValue(
      String command, String expected) {
    Map<String, String> envVariables =
        Map.of(
            "USER", "John",
            "PATH", "/usr/bin");
    resolver = EnvVarResolver.of(envVariables);
    var result = resolver.resolve(command);
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "${TOKEN:default}", // Single colon no longer supported
        "${MISSING}", // Still fails because no default
        "Hi ${MISSING}!", // Same
        "${A:-} ${B}" // A has empty default, B is missing
      })
  void givenMissingEnvWithoutDefault_whenResolve_thenThrowException(String command) {
    Map<String, String> envVariables = Map.of("A", "something");
    resolver = EnvVarResolver.of(envVariables);
    assertThatThrownBy(() -> resolver.resolve(command))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("referenced, but not found");
  }

  @ParameterizedTest
  @CsvSource({
    "'${TOKEN:-abc:def}', 'abc:def'", // Bash-style with colon in fallback
    "'${TOKEN:=abc:def}', 'abc:def'", // Bash-style with colon in fallback
    "'${TOKEN:-abc:def:ghi}', 'abc:def:ghi'", // Bash-style
    "'${TOKEN:-}', ''", // Bash-style colon-dash at the end
    "'${TOKEN:=}', ''" // Bash-style colon-equals at the end
  })
  void givenFallbackWithColons_whenResolve_thenParseCorrectly(String command, String expected) {
    Map<String, String> envVariables = Map.of(); // No TOKEN set
    resolver = EnvVarResolver.of(envVariables);
    var result = resolver.resolve(command);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void givenNullOrBlankSource_whenResolve_thenReturnSource() {
    resolver = EnvVarResolver.of(Map.of());

    assertThat(resolver.resolve(null)).isNull();
    assertThat(resolver.resolve("   ")).isEqualTo("   ");
  }

  @Test
  void givenNonStrictResolver_whenMissingEnvVariables_thenLeavesPlaceholdersUnresolved() {
    resolver = EnvVarResolver.of(Map.of("USER", "John"), false);

    var result = resolver.resolve("Hello ${USER}, ${MISSING}!");

    assertThat(result).isEqualTo("Hello John, ${MISSING}!");
  }

  @Test
  void givenDeploymentDefaults_whenResolve_thenUseDefaultsAndSuppliedValues() {
    resolver =
        EnvVarResolver.withDeploymentDefaults(
            Map.of(
                "DEPLOYMENT_ID", "deployment-1",
                "USER", "John"));

    var result = resolver.resolve("${DEPLOYMENT_ID}|${DEPLOYMENT_TIMESTAMP}|${USER}");
    var parts = result.split("\\|");

    assertThat(parts).hasSize(3);
    assertThat(parts[0]).isEqualTo("deployment-1");
    assertThat(Long.parseLong(parts[1])).isGreaterThan(0);
    assertThat(parts[2]).isEqualTo("John");
  }

  @Test
  void givenNonStrictDeploymentDefaults_whenMissingNonDefaultEnvVariable_thenLeavesPlaceholder() {
    resolver =
        EnvVarResolver.withDeploymentDefaults(Map.of("DEPLOYMENT_ID", "deployment-1"), false);

    var result = resolver.resolve("${DEPLOYMENT_ID}|${MISSING}");

    assertThat(result).isEqualTo("deployment-1|${MISSING}");
  }

  @Test
  void givenJsonSource_whenResolveInJson_thenResolveStringLeafNodes() throws IOException {
    resolver = EnvVarResolver.of(Map.of("USER", "John"));

    var result =
        resolver.resolveInJson(
            "{\"user\":\"${USER}\",\"nested\":{\"path\":\"${PATH:-/tmp}\"},\"count\":1}");
    var json = new ObjectMapper().readTree(result);

    assertThat(json.get("user").asText()).isEqualTo("John");
    assertThat(json.get("count").asInt()).isEqualTo(1);
    assertThat(json.get("nested").get("path").asText()).isEqualTo("/tmp");
  }
}
