/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class DeserFailureHandlerTest {

  @Mock private ConsumerRecord<byte[], byte[]> mockRecord;
  @Mock private DeserFailureHandler.DeserializationCaller mockDeserCaller;
  @Mock private DeserFailureProducer mockProducer;

  @BeforeEach
  void setup() {
    mockRecord = mock(ConsumerRecord.class);
    mockDeserCaller = mock(DeserFailureHandler.DeserializationCaller.class);
    mockProducer = mock(DeserFailureProducer.class);

    when(mockRecord.topic()).thenReturn("input-topic");
    when(mockRecord.partition()).thenReturn(0);
    when(mockRecord.offset()).thenReturn(123L);
  }

  @Test
  void shouldThrowExceptionWhenHandlerTypeIsNone() throws Exception {
    DeserFailureHandler handler = new DeserFailureHandler(DeserFailureHandlerType.NONE, null);
    IOException ioException = new IOException("Failed to deserialize");

    doThrow(ioException).when(mockDeserCaller).call();

    assertThatThrownBy(() -> handler.deserWithFailureHandling(mockRecord, mockDeserCaller))
        .isInstanceOf(IOException.class)
        .hasMessage("Failed to deserialize");
  }

  @Test
  void shouldLogWhenHandlerTypeIsLog() throws Exception {
    DeserFailureHandler handler = new DeserFailureHandler(DeserFailureHandlerType.LOG, null);

    doThrow(new IOException("Failure")).when(mockDeserCaller).call();

    // This only ensures no exception is thrown, log output would need SLF4J test tools to verify
    assertThatCode(() -> handler.deserWithFailureHandling(mockRecord, mockDeserCaller))
        .doesNotThrowAnyException();
  }

  @Test
  void shouldSendToKafkaWhenHandlerTypeIsKafka() throws Exception {
    DeserFailureHandler handler =
        new DeserFailureHandler(DeserFailureHandlerType.KAFKA, mockProducer);

    doThrow(new IOException("Failure")).when(mockDeserCaller).call();

    handler.deserWithFailureHandling(mockRecord, mockDeserCaller);

    verify(mockProducer).send(mockRecord);
  }
}
