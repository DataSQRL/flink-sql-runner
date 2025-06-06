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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

public class DeserFailureProducerTest {

  private static final String TOPIC = "test-topic";
  private Properties consumerProps;

  @BeforeEach
  void setUp() {
    consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", "localhost:9092");
  }

  @Test
  void send_shouldInitializeKafkaProducerAndSendRecord() {
    try (MockedConstruction<KafkaProducer> mocked = mockConstruction(KafkaProducer.class)) {
      DeserFailureProducer producer = new DeserFailureProducer(TOPIC, consumerProps);

      byte[] key = "key".getBytes();
      byte[] value = "value".getBytes();
      ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(TOPIC, 0, 0L, key, value);

      producer.send(record);

      KafkaProducer<byte[], byte[]> kafkaProducer = mocked.constructed().get(0);
      verify(kafkaProducer).send(any(ProducerRecord.class));
    }
  }

  @Test
  void send_shouldLogWhenRecordIsNullAndNotSendAnything() {
    try (MockedConstruction<KafkaProducer> mocked = mockConstruction(KafkaProducer.class)) {
      DeserFailureProducer producer = new DeserFailureProducer(TOPIC, consumerProps);
      producer.send(null);

      KafkaProducer<byte[], byte[]> kafkaProducer = mocked.constructed().get(0);
      verify(kafkaProducer, never()).send(any());
    }
  }
}
