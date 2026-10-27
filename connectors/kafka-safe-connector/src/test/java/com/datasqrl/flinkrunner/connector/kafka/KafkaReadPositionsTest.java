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

import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaReadPositionsTest {

  private final String topic = "topic-" + UUID.randomUUID();

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
