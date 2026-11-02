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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;

/**
 * Where a split that starts at a placeholder starts reading, as its consumer works it out. Only a
 * fresh start hands out placeholders: {@link KafkaPartitionSplit#EARLIEST_OFFSET}, or {@link
 * KafkaPartitionSplit#COMMITTED_OFFSET} for the consumer group's offsets. A restored split carries
 * an offset.
 */
final class KafkaStartingOffsets {

  private KafkaStartingOffsets() {}

  /**
   * Resolves each of {@code placeholders} to an offset: the start of the log for {@code
   * EARLIEST_OFFSET}; for {@code COMMITTED_OFFSET}, the consumer group's committed offset, or else
   * where {@code auto.offset.reset} sends the consumer. Leaves out what it cannot resolve, such as
   * a partition without a committed offset under {@code auto.offset.reset=none}.
   */
  static Map<TopicPartition, Long> resolve(
      Admin admin,
      Properties consumerProperties,
      Map<TopicPartition, Long> placeholders,
      long timeoutMillis)
      throws Exception {
    var committed =
        placeholders.containsValue(KafkaPartitionSplit.COMMITTED_OFFSET)
            ? committedOffsets(admin, consumerProperties, timeoutMillis)
            : Map.<TopicPartition, OffsetAndMetadata>of();

    var startingOffsets = new HashMap<TopicPartition, Long>();
    var logOffsets = new HashMap<TopicPartition, OffsetSpec>();
    placeholders.forEach(
        (partition, placeholder) -> {
          var committedOffset =
              placeholder == KafkaPartitionSplit.COMMITTED_OFFSET ? committed.get(partition) : null;
          if (committedOffset != null) {
            startingOffsets.put(partition, committedOffset.offset());
            return;
          }

          var logOffset = logOffset(placeholder, consumerProperties);
          if (logOffset != null) {
            logOffsets.put(partition, logOffset);
          }
        });

    if (!logOffsets.isEmpty()) {
      admin
          .listOffsets(logOffsets, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
          .all()
          .get(timeoutMillis, TimeUnit.MILLISECONDS)
          .forEach((partition, info) -> startingOffsets.put(partition, info.offset()));
    }

    return startingOffsets;
  }

  private static Map<TopicPartition, OffsetAndMetadata> committedOffsets(
      Admin admin, Properties consumerProperties, long timeoutMillis) throws Exception {
    String groupId = consumerProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    if (groupId == null) {
      return Map.of();
    }

    // A partition without a committed offset maps to null, as an absent one does.
    return admin
        .listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata()
        .get(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Where in the log a consumer starts that has no committed offset to start at, or null where it
   * fails instead, or where this cannot tell.
   */
  @Nullable
  private static OffsetSpec logOffset(long placeholder, Properties consumerProperties) {
    if (placeholder == KafkaPartitionSplit.EARLIEST_OFFSET) {
      return OffsetSpec.earliest();
    }
    if (placeholder != KafkaPartitionSplit.COMMITTED_OFFSET) {
      return null;
    }

    String reset = consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    return switch (reset.trim().toLowerCase(Locale.ROOT)) {
      case "earliest" -> OffsetSpec.earliest();
      // Records appended since the consumer reset are new: the safety margin covers the time they
      // take to be read.
      case "latest" -> OffsetSpec.latest();
      default -> null;
    };
  }
}
