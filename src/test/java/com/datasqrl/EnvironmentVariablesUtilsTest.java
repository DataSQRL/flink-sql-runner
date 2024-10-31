package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;
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
    String result = EnvironmentVariablesUtils.replaceWithEnv(command, envVariables);
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
    assertThatThrownBy(() -> EnvironmentVariablesUtils.replaceWithEnv(command, envVariables))
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
    assertThatThrownBy(() -> EnvironmentVariablesUtils.replaceWithEnv(command, envVariables))
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
    Set<String> missingVars =
        EnvironmentVariablesUtils.validateEnvironmentVariables(envVariables, script);
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
    Set<String> missingVars =
        EnvironmentVariablesUtils.validateEnvironmentVariables(envVariables, script);
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
    Set<String> missingVars =
        EnvironmentVariablesUtils.validateEnvironmentVariables(envVariables, script);
    assertThat(missingVars)
        .containsExactly("USER_NAME"); // Ensure only USERNAME is reported missing
  }
}
