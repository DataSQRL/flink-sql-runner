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
package com.datasqrl.flinkrunner.stdlib.openai;

import static com.datasqrl.flinkrunner.stdlib.openai.utils.FunctionMetricTracker.CALL_COUNT;
import static com.datasqrl.flinkrunner.stdlib.openai.utils.FunctionMetricTracker.ERROR_COUNT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.stream.Stream;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.functions.FunctionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExtractJsonTest {

  @Mock private HttpClient mockHttpClient;

  @Mock private HttpResponse<String> mockHttpResponse;

  @InjectMocks private OpenAiCompletions openAiCompletions;

  @Mock private FunctionContext functionContext;

  @Mock private MetricGroup metricGroup;

  @Mock private Counter callCounter;

  @Mock private Counter errorCounter;

  private extract_json function;

  @BeforeEach
  void setUp() throws Exception {
    final String functionName = extract_json.class.getSimpleName();

    when(functionContext.getMetricGroup()).thenReturn(metricGroup);
    when(metricGroup.counter(eq(format(CALL_COUNT, functionName)))).thenReturn(callCounter);
    when(metricGroup.counter(eq(format(ERROR_COUNT, functionName)))).thenReturn(errorCounter);

    function =
        new extract_json() {
          @Override
          public OpenAiCompletions createOpenAiCompletions() {
            return openAiCompletions;
          }
        };
    function.open(functionContext);
  }

  @Test
  void testEvalSuccessfulCompletion() throws IOException, InterruptedException {
    String responseBody =
        "{\n"
            + "  \"choices\": [\n"
            + "    {\n"
            + "      \"message\": {\n"
            + "        \"content\": \"Hello.\"\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";

    String expectedResponse = "Hello.";

    // Configure the mock to return a successful response
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseBody);

    String result = function.eval("prompt", "model");

    verify(callCounter, times(1)).inc();
    verify(errorCounter, never()).inc();

    assertThat(result).isEqualTo(expectedResponse);
  }

  @Test
  void testEvalWithDefaults() throws IOException, InterruptedException {
    String responseBody =
        "{\n"
            + "  \"choices\": [\n"
            + "    {\n"
            + "      \"message\": {\n"
            + "        \"content\": \"Hello.\"\n"
            + "      }\n"
            + "    }\n"
            + "  ]\n"
            + "}\n";

    String expectedResponse = "Hello.";

    // Configure the mock to return a successful response with defaults
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseBody);

    String result = function.eval("prompt", "model");

    verify(callCounter, times(1)).inc();
    verify(errorCounter, never()).inc();

    assertThat(result).isEqualTo(expectedResponse);
  }

  @Test
  void testEvalErrorHandling() throws IOException, InterruptedException {
    IOException exception = new IOException("Test Exception");

    // Configure the mock to throw an IOException, simulating repeated failures
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(exception);

    assertThatThrownBy(() -> function.eval("prompt", "model")).hasRootCause(exception);

    verify(mockHttpClient, times(1))
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    verify(callCounter, times(1)).inc();
    verify(errorCounter, times(1)).inc();
  }

  @ParameterizedTest
  @MethodSource("provideInvalidTestArguments")
  void testEvalWhenInputIsInvalid(String prompt, String modelName)
      throws IOException, InterruptedException {
    assertThat(function.eval(prompt, modelName)).isNull();

    verify(mockHttpClient, never())
        .send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  private static Stream<Arguments> provideInvalidTestArguments() {
    return Stream.of(Arguments.of(null, null), Arguments.of("", null), Arguments.of(null, ""));
  }
}
