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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.kafka.common.TopicPartition;

/**
 * How far the Kafka source on the current thread has read each partition.
 *
 * <p>Flink gives a watermark generator only rows and their timestamps: not its partition, and
 * nothing for records that produce no row, such as records sent to the dead-letter queue or
 * tombstones in {@code kafka-safe}. So {@code SafeDynamicKafkaDeserializationSchema} records every
 * consumed record here and {@link KafkaRecordTimestampWatermarkStrategy} reads it back. The two are
 * serialized separately and share only the source task's thread: {@code KafkaRecordEmitter}
 * deserializes each record on it and the rows reach {@code WatermarkGenerator#onEvent} before
 * deserialization returns, so during a generator's {@code onEvent} its own partition is {@link
 * #latest()}.
 *
 * <p>Offsets that hold no record, such as transaction markers, never reach the deserializer. How
 * far the consumer has moved past them, {@link KafkaFetchPositions} learns.
 *
 * <p>A generator learns its partition only from its first row, so {@link AssignmentRecordingSource}
 * also records here every split assigned to the reader, with the offset it starts at.
 */
@Internal
public final class KafkaReadPositions {

  /**
   * The offset after the last record, before there is one: what a split has consumed, or its
   * consumer returned, before the first record.
   */
  static final long BEFORE_FIRST_RECORD = -1L;

  /** The starting offset of a partition no split of which has been assigned to the reader. */
  private static final long NOT_ASSIGNED = Long.MIN_VALUE;

  private static final ThreadLocal<KafkaReadPositions> FOR_THREAD =
      ThreadLocal.withInitial(KafkaReadPositions::new);

  private final Map<TopicPartition, ReadPosition> positions = new HashMap<>();

  /** The position of the partition this thread consumed a record from last. */
  @Nullable private ReadPosition latest;

  private KafkaReadPositions() {}

  /** Records that the record at {@code offset} has been consumed, before it is deserialized. */
  public static void consumed(String topic, int partition, long offset) {
    FOR_THREAD.get().consume(topic, partition, offset);
  }

  /** The position of the partition this thread consumed a record from last. */
  @Nullable
  public static ReadPosition latest() {
    return FOR_THREAD.get().latest;
  }

  /**
   * Records that a split of {@code partition} has been assigned to the reader on this thread. Its
   * {@code startingOffset} is an offset, or a {@code KafkaPartitionSplit} placeholder such as
   * {@code EARLIEST_OFFSET}.
   */
  public static void assigned(TopicPartition partition, long startingOffset) {
    FOR_THREAD.get().positions.computeIfAbsent(partition, ReadPosition::new).startingOffset =
        startingOffset;
  }

  /** The positions of the partitions assigned to the reader on this thread. */
  static List<ReadPosition> assignments() {
    return FOR_THREAD.get().positions.values().stream()
        .filter(position -> position.startingOffset != NOT_ASSIGNED)
        .toList();
  }

  /** Forgets this thread's positions, as the thread of a new task starts without any. */
  @VisibleForTesting
  static void clear() {
    FOR_THREAD.remove();
  }

  private void consume(String topic, int partition, long offset) {
    // Records arrive in runs from one partition, which mostly spares the lookup.
    if (latest == null || !latest.isFor(topic, partition)) {
      latest = positions.computeIfAbsent(new TopicPartition(topic, partition), ReadPosition::new);
    }

    latest.nextOffset = offset + 1;
  }

  /** How far one partition has been read. */
  public static final class ReadPosition {

    private final TopicPartition partition;

    /** The offset after the last consumed record. */
    private long nextOffset = BEFORE_FIRST_RECORD;

    /** Where the partition's split starts reading, if one has been assigned to the reader. */
    private long startingOffset = NOT_ASSIGNED;

    private ReadPosition(TopicPartition partition) {
      this.partition = partition;
    }

    public TopicPartition partition() {
      return partition;
    }

    public long nextOffset() {
      return nextOffset;
    }

    /**
     * Where the partition's split starts reading: an offset, a {@code KafkaPartitionSplit}
     * placeholder not resolved yet, or {@link Long#MIN_VALUE} if no split of it has been assigned
     * to the reader. Placeholders are negative, as is {@code MIN_VALUE}.
     */
    public long startingOffset() {
      return startingOffset;
    }

    /** Whether the partition's split starts at a placeholder not resolved yet. */
    boolean startsAtPlaceholder() {
      return startingOffset < 0 && startingOffset != NOT_ASSIGNED;
    }

    /** Records the offset that the placeholder the partition's split starts at stands for. */
    void resolveStartingOffset(long offset) {
      startingOffset = offset;
    }

    private boolean isFor(String topic, int partition) {
      return this.partition.partition() == partition && this.partition.topic().equals(topic);
    }
  }
}
