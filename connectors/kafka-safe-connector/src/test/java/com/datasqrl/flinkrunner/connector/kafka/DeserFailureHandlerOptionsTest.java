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
package com.datasqrl.flinkrunner.connector.kafka;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.stream.Stream;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DeserFailureHandlerOptionsTest {

  @ParameterizedTest(name = "Should pass validation: handler={0}, topic=\"{1}\"")
  @MethodSource("validConfigs")
  void shouldNotThrowForValidConfigs(DeserFailureHandlerType handlerType, String topic) {
    ReadableConfig config = mock(ReadableConfig.class);
    when(config.get(DeserFailureHandlerOptions.SCAN_DESER_FAILURE_HANDLER)).thenReturn(handlerType);
    when(config.get(DeserFailureHandlerOptions.SCAN_DESER_FAILURE_TOPIC)).thenReturn(topic);

    assertThatCode(() -> DeserFailureHandlerOptions.validateDeserFailureHandlerOptions(config))
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> validConfigs() {
    return Stream.of(
        Arguments.of(DeserFailureHandlerType.NONE, null),
        Arguments.of(DeserFailureHandlerType.NONE, " "),
        Arguments.of(DeserFailureHandlerType.LOG, null),
        Arguments.of(DeserFailureHandlerType.KAFKA, "dead-letter-topic"));
  }

  @ParameterizedTest(name = "Should fail: handler={0}, topic=\"{1}\"")
  @MethodSource("invalidConfigs")
  void shouldThrowForInvalidConfigs(
      DeserFailureHandlerType handlerType, String topic, String expectedMessage) {
    ReadableConfig config = mock(ReadableConfig.class);
    when(config.get(DeserFailureHandlerOptions.SCAN_DESER_FAILURE_HANDLER)).thenReturn(handlerType);
    when(config.get(DeserFailureHandlerOptions.SCAN_DESER_FAILURE_TOPIC)).thenReturn(topic);

    assertThatThrownBy(() -> DeserFailureHandlerOptions.validateDeserFailureHandlerOptions(config))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(expectedMessage);
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of(
            DeserFailureHandlerType.KAFKA, "   ", "'scan.deser-failure.topic' is not specified"),
        Arguments.of(
            DeserFailureHandlerType.KAFKA, null, "'scan.deser-failure.topic' is not specified"),
        Arguments.of(
            DeserFailureHandlerType.LOG, "extra-topic", "'scan.deser-failure.topic' is specified"),
        Arguments.of(
            DeserFailureHandlerType.NONE, "some-topic", "'scan.deser-failure.topic' is specified"));
  }
}
