/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EnvironmentVariablesUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "'Hello, ${USER}', 'Hello, John'", // Positive case: USER variable exists
    "'Path: ${PATH}', 'Path: /usr/bin'", // Positive case: PATH variable exists
    "'No match here', 'No match here'", // Case with no placeholders
    "'Partial ${USER_HOME', 'Partial ${USER_HOME'" // Case to ensure partial match is not replaced
  })
  void givenEnvVariables_whenReplaceWithEnv_thenReplaceCorrectly(String command, String expected) {
    Map<String, String> envVariables =
        Map.of(
            "USER", "John",
            "PATH", "/usr/bin");
    var result = EnvVarUtils.replaceWithEnv(command, envVariables);
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "'Hello, ${USER}'", // USER variable missing
    "'Path: ${UNKNOWN}'", // UNKNOWN variable missing
    "'Combined ${VAR1} and ${VAR2}'" // Multiple missing variables
  })
  void givenMissingEnvVariables_whenReplaceWithEnv_thenThrowException(String command) {
    Map<String, String> envVariables = Map.of(); // Empty map to simulate missing variables
    assertThatThrownBy(() -> EnvVarUtils.replaceWithEnv(command, envVariables))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Missing environment variable");
  }

  @ParameterizedTest
  @CsvSource({
    "'${USER} is different from ${USER_NAME}'",
    "'${NAME} is different from ${USER_NAME}'"
  })
  void givenSimilarEnvVariableNames_whenReplaceWithEnv_thenPartialMatchDoesNotOccur(
      String command) {
    Map<String, String> envVariables =
        Map.of(
            "USER", "John",
            "NAME", "exists");
    assertThatThrownBy(() -> EnvVarUtils.replaceWithEnv(command, envVariables))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Missing environment variable: USER_NAME");
  }

  @ParameterizedTest
  @CsvSource({"'Run with ${VAR1} and ${VAR2}'", "'Execute ${USER} ${HOME}'", "'Simple command'"})
  void givenAllEnvVariablesInScript_whenValidateEnvironmentVariables_thenReturnEmptySet(
      String script) {
    Map<String, String> envVariables =
        Map.of(
            "VAR1", "1",
            "VAR2", "2",
            "USER", "admin",
            "HOME", "/home/admin");
    var missingVars = EnvVarUtils.validateEnvironmentVariables(envVariables, script);
    assertThat(missingVars).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({
    "'Run with ${VAR1} and ${VAR2}', 'VAR2'",
    "'Execute ${USER} ${HOME} ${PATH}', 'PATH'",
    "'${MISSING_VAR}', 'MISSING_VAR'"
  })
  void givenSomeEnvVariablesInScript_whenValidateEnvironmentVariables_thenReturnMissingVariables(
      String script, String expectedMissing) {
    Map<String, String> envVariables =
        Map.of(
            "VAR1", "1",
            "USER", "admin",
            "HOME", "/home/admin");
    var missingVars = EnvVarUtils.validateEnvironmentVariables(envVariables, script);
    assertThat(missingVars).containsExactlyInAnyOrder(expectedMissing.split(","));
  }

  @ParameterizedTest
  @CsvSource({
    "'${USER} is different from ${USER_NAME}'",
    "'${NAME} is different from ${USER_NAME}'"
  })
  void givenSimilarVariableNames_whenValidateEnvironmentVariables_thenNoPartialMatch(
      String script) {
    Map<String, String> envVariables =
        Map.of(
            "USER", "admin",
            "NAME", "admin");
    var missingVars = EnvVarUtils.validateEnvironmentVariables(envVariables, script);
    assertThat(missingVars)
        .containsExactly("USER_NAME"); // Ensure only USERNAME is reported missing
  }
}
