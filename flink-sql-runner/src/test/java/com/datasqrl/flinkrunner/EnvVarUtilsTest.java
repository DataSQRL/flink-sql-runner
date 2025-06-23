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
package com.datasqrl.flinkrunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class EnvVarUtilsTest {

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
    var result = EnvVarUtils.resolveEnvVars(command, envVariables);
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
    Map<String, String> envVariables = Map.of(); // Empty map to simulate missing variables
    assertThatThrownBy(() -> EnvVarUtils.resolveEnvVars(command, envVariables))
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
    assertThatThrownBy(() -> EnvVarUtils.resolveEnvVars(command, envVariables))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The following environment variables were referenced, but not found: [USER_NAME]");
  }
}
