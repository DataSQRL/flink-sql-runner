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
import static org.assertj.core.api.Assertions.tuple;

import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import java.util.UUID;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaReadPositionsTest {

  private final String topic = "topic-" + UUID.randomUUID();

  @AfterEach
  void clearReadPositions() {
    KafkaReadPositions.clear();
  }

  @Test
  void testRecordsAssignedPartitionsWithWhereTheirSplitsStart() {
    var restored = new TopicPartition(topic, 0);
    var fresh = new TopicPartition(topic, 1);
    KafkaReadPositions.assigned(restored, 42L);
    KafkaReadPositions.assigned(fresh, KafkaPartitionSplit.EARLIEST_OFFSET);
    // A record the deserializer sees does not make its partition an assignment.
    consume(2, 7L);

    assertThat(KafkaReadPositions.assignments())
        .extracting(ReadPosition::partition, ReadPosition::startingOffset)
        .containsExactlyInAnyOrder(
            tuple(restored, 42L), tuple(fresh, KafkaPartitionSplit.EARLIEST_OFFSET));
    assertThat(KafkaReadPositions.assignments())
        .allMatch(position -> position.nextOffset() == KafkaReadPositions.BEFORE_FIRST_RECORD);
  }

  @Test
  void testConsumingAssignedPartitionKeepsWhereItsSplitStarted() {
    KafkaReadPositions.assigned(new TopicPartition(topic, 0), 42L);

    var position = consume(0, 42L);

    assertThat(position.startingOffset()).isEqualTo(42L);
    assertThat(position.nextOffset()).isEqualTo(43L);
    assertThat(KafkaReadPositions.assignments()).containsExactly(position);
  }

  @Test
  void testLatestIsPositionOfPartitionConsumedLast() {
    var position = consume(3, 41L);

    assertThat(position.partition()).isEqualTo(new TopicPartition(topic, 3));
    assertThat(position.nextOffset()).isEqualTo(42L);
  }

  @Test
  void testKeepsPartitionsApart() {
    var partition0 = consume(0, 10L);
    var partition1 = consume(1, 20L);
    consume(0, 11L);
    consume(1, 21L);

    assertThat(partition0.nextOffset()).isEqualTo(12L);
    assertThat(partition1.nextOffset()).isEqualTo(22L);
  }

  private ReadPosition consume(int partition, long offset) {
    KafkaReadPositions.consumed(topic, partition, offset);
    return KafkaReadPositions.latest();
  }
}
