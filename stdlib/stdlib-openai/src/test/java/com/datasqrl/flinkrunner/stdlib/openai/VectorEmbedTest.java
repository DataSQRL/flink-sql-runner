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

import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
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
class VectorEmbedTest {

  @Mock private HttpClient httpClient;

  @Mock private HttpResponse<String> httpResponse;

  @InjectMocks private OpenAiEmbeddings openAiEmbeddings;

  @Mock private FunctionContext functionContext;

  @Mock private MetricGroup metricGroup;

  @Mock private Counter callCounter;

  @Mock private Counter errorCounter;

  private vector_embed function;

  @BeforeEach
  void setUp() throws Exception {
    final String functionName = vector_embed.class.getSimpleName();

    when(functionContext.getMetricGroup()).thenReturn(metricGroup);
    when(metricGroup.counter(eq(format(CALL_COUNT, functionName)))).thenReturn(callCounter);
    when(metricGroup.counter(eq(format(ERROR_COUNT, functionName)))).thenReturn(errorCounter);

    function =
        new vector_embed() {
          @Override
          protected OpenAiEmbeddings createOpenAiEmbeddings() {
            return openAiEmbeddings;
          }
        };

    function.open(functionContext);
  }

  @Test
  void testEvalSuccessfulEmbedding() throws IOException, InterruptedException {
    // Mock response data
    String mockResponse = "{\"data\": [{\"embedding\": [0.1, 0.2, 0.3]}]}";
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn(mockResponse);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(httpResponse);

    // Execute function
    FlinkVectorType result = function.eval("some text", "model-name");

    verify(callCounter, times(1)).inc();
    verify(errorCounter, never()).inc();

    // Verify the result
    assertThat(result.getValue()).containsExactly(0.1, 0.2, 0.3);
  }

  @Test
  void testEvalErrorHandling() throws IOException, InterruptedException {
    IOException exception = new IOException("Test Exception");

    // Mock the HttpClient to throw an IOException for retries
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(exception);

    // Attempt to call vectorEmbed, expecting retries
    assertThatThrownBy(() -> function.eval("some text", "model-name")).hasRootCause(exception);

    verify(httpClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));

    verify(callCounter, times(1)).inc();
    verify(errorCounter, times(1)).inc();
  }

  @ParameterizedTest
  @MethodSource("provideInvalidTestArguments")
  void testEvalWhenInputIsInvalid(String prompt, String modelName)
      throws IOException, InterruptedException {
    assertThat(function.eval(prompt, modelName)).isNull();
    verify(httpClient, never()).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  private static Stream<Arguments> provideInvalidTestArguments() {
    return Stream.of(Arguments.of(null, null), Arguments.of("", null), Arguments.of(null, ""));
  }
}
