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
package com.datasqrl.flinkrunner.connector.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flinkrunner.connector.kafka.KafkaFetchPositions.FetchPosition;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaFetchPositionsTest {

  private static final TopicPartition PARTITION = new TopicPartition("topic", 0);

  private final String sourceId = UUID.randomUUID().toString();

  @Test
  void testPublishesPositionAPollMovedToWithoutReturningRecords() {
    // Moving past transaction markers returns no record. The records returned before tie the
    // position to how far a split has read; before the first of them, nothing does.
    var interceptor = interceptor(sourceId);

    interceptor.onConsume(poll(List.of(), 10L));

    assertThat(KafkaFetchPositions.fetchPosition(sourceId, PARTITION)).isNull();

    interceptor.onConsume(poll(List.of(10L, 11L, 12L), 13L));

    assertThat(KafkaFetchPositions.fetchPosition(sourceId, PARTITION))
        .isEqualTo(new FetchPosition(13L, 13L));

    interceptor.onConsume(poll(List.of(), 15L));

    assertThat(KafkaFetchPositions.fetchPosition(sourceId, PARTITION))
        .isEqualTo(new FetchPosition(13L, 15L));
  }

  @Test
  void testClosingLeavesPositionOfConsumerThatTookOver() {
    // After a failover, the consumer that reads the partition now may publish, even the same
    // position, before the previous one closes.
    var previous = interceptor(sourceId);
    previous.onConsume(poll(List.of(10L), 11L));
    var current = interceptor(sourceId);
    current.onConsume(poll(List.of(10L), 11L));

    previous.close();

    assertThat(KafkaFetchPositions.fetchPosition(sourceId, PARTITION))
        .isEqualTo(new FetchPosition(11L, 11L));

    current.close();

    assertThat(KafkaFetchPositions.fetchPosition(sourceId, PARTITION)).isNull();
  }

  @Test
  void testPublishingConsumerKeepsInterceptorsAlreadyConfigured() {
    var properties = new Properties();
    properties.setProperty("bootstrap.servers", "broker:9092");
    properties.setProperty("interceptor.classes", "com.example.AuditInterceptor");

    var publishing = KafkaFetchPositions.publishing(properties, sourceId);

    assertThat(publishing.getProperty("bootstrap.servers")).isEqualTo("broker:9092");
    assertThat(publishing.getProperty("interceptor.classes"))
        .isEqualTo(
            "com.example.AuditInterceptor,"
                + "com.datasqrl.flinkrunner.connector.kafka.KafkaFetchPositions");
    assertThat(publishing.getProperty("datasqrl.fetch-positions.source-id")).isEqualTo(sourceId);
    assertThat(properties.getProperty("interceptor.classes"))
        .isEqualTo("com.example.AuditInterceptor");
  }

  private static KafkaFetchPositions interceptor(String sourceId) {
    var interceptor = new KafkaFetchPositions();
    interceptor.configure(Map.of(KafkaFetchPositions.SOURCE_ID_CONFIG, sourceId));
    return interceptor;
  }

  /** What a poll hands the interceptors: the records it returns, and where it left the position. */
  private static ConsumerRecords<byte[], byte[]> poll(List<Long> offsets, long position) {
    List<ConsumerRecord<byte[], byte[]>> records =
        offsets.stream()
            .map(
                offset ->
                    new ConsumerRecord<byte[], byte[]>(
                        PARTITION.topic(), PARTITION.partition(), offset, null, null))
            .toList();
    return new ConsumerRecords<>(
        records.isEmpty() ? Map.of() : Map.of(PARTITION, records),
        Map.of(PARTITION, new OffsetAndMetadata(position)));
  }
}
