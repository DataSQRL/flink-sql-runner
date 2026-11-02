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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaStartingOffsetsTest {

  private static final TopicPartition PARTITION_0 = new TopicPartition("topic", 0);
  private static final TopicPartition PARTITION_1 = new TopicPartition("topic", 1);

  /** Every log here starts at offset 5 and ends at offset 50. */
  private static final long LOG_START = 5L;

  private static final long LOG_END = 50L;

  private final Admin admin = mock(Admin.class);
  private final Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();

  @Test
  void testEarliestStartsAtLogStart() throws Exception {
    stubAdmin();

    assertThat(resolve(properties(null), Map.of(PARTITION_0, KafkaPartitionSplit.EARLIEST_OFFSET)))
        .containsExactly(Map.entry(PARTITION_0, LOG_START));
    verify(admin, never()).listConsumerGroupOffsets(anyString());
  }

  @Test
  void testCommittedStartsAtCommittedOffset() throws Exception {
    stubAdmin();
    committed.put(PARTITION_0, new OffsetAndMetadata(17L));

    assertThat(resolve(properties("none"), committed(PARTITION_0)))
        .containsExactly(Map.entry(PARTITION_0, 17L));
  }

  @Test
  void testEarliestIgnoresCommittedOffset() throws Exception {
    // A source started from the group's offsets reads partitions it discovers later from their
    // start, so both placeholders can come together.
    stubAdmin();
    committed.put(PARTITION_0, new OffsetAndMetadata(17L));
    committed.put(PARTITION_1, new OffsetAndMetadata(30L));
    var placeholders = committed(PARTITION_0);
    placeholders.put(PARTITION_1, KafkaPartitionSplit.EARLIEST_OFFSET);

    assertThat(resolve(properties("none"), placeholders))
        .containsExactlyInAnyOrderEntriesOf(Map.of(PARTITION_0, 17L, PARTITION_1, LOG_START));
  }

  @Test
  void testCommittedWithoutCommitFallsBackToOffsetReset() throws Exception {
    stubAdmin();
    committed.put(PARTITION_0, new OffsetAndMetadata(17L));

    assertThat(resolve(properties("earliest"), committed(PARTITION_0, PARTITION_1)))
        .containsExactlyInAnyOrderEntriesOf(Map.of(PARTITION_0, 17L, PARTITION_1, LOG_START));
    assertThat(resolve(properties("LATEST"), committed(PARTITION_1)))
        .containsExactly(Map.entry(PARTITION_1, LOG_END));
  }

  @Test
  void testLeavesOutWhatTheConsumerFailsOn() throws Exception {
    // Without a committed offset or a reset strategy, the consumer fails rather than start.
    stubAdmin();

    assertThat(resolve(properties("none"), committed(PARTITION_0))).isEmpty();
    assertThat(resolve(properties("by_duration:PT1H"), committed(PARTITION_0))).isEmpty();
  }

  @Test
  void testWithoutGroupFallsBackToOffsetReset() throws Exception {
    stubAdmin();
    var properties = properties("earliest");
    properties.remove("group.id");

    assertThat(resolve(properties, committed(PARTITION_0)))
        .containsExactly(Map.entry(PARTITION_0, LOG_START));
    verify(admin, never()).listConsumerGroupOffsets(anyString());
  }

  private Map<TopicPartition, Long> resolve(
      Properties properties, Map<TopicPartition, Long> placeholders) throws Exception {
    return KafkaStartingOffsets.resolve(admin, properties, placeholders, 1_000L);
  }

  private static Map<TopicPartition, Long> committed(TopicPartition... partitions) {
    var placeholders = new HashMap<TopicPartition, Long>();
    for (TopicPartition partition : partitions) {
      placeholders.put(partition, KafkaPartitionSplit.COMMITTED_OFFSET);
    }
    return placeholders;
  }

  private static Properties properties(String offsetReset) {
    var properties = new Properties();
    properties.setProperty("group.id", "group");
    if (offsetReset != null) {
      properties.setProperty("auto.offset.reset", offsetReset);
    }
    return properties;
  }

  private void stubAdmin() {
    var committedResult = mock(ListConsumerGroupOffsetsResult.class);
    when(committedResult.partitionsToOffsetAndMetadata())
        .thenAnswer(invocation -> KafkaFuture.completedFuture(committed));
    when(admin.listConsumerGroupOffsets("group")).thenReturn(committedResult);
    when(admin.listOffsets(anyMap(), any(ListOffsetsOptions.class)))
        .thenAnswer(invocation -> listOffsets(invocation.getArgument(0)));
  }

  private static ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> request) {
    var futures = new HashMap<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>();
    request.forEach(
        (partition, spec) -> {
          long offset = spec instanceof OffsetSpec.EarliestSpec ? LOG_START : LOG_END;
          futures.put(
              partition,
              KafkaFuture.completedFuture(
                  new ListOffsetsResultInfo(offset, -1L, Optional.empty())));
        });
    return new ListOffsetsResult(futures);
  }
}
