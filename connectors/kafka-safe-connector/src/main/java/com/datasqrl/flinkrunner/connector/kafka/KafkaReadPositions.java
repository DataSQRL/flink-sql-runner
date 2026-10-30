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
 * <p>Until its first row, a split's generator can only know it reads one of the partitions {@link
 * #assigned} to the reader on this thread.
 */
@Internal
public final class KafkaReadPositions {

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
   * Records that the reader on this thread was assigned the split of {@code partition}, which
   * starts at {@code startingOffset}: an offset, or one of {@code KafkaPartitionSplit}'s markers.
   */
  public static void assigned(String topic, int partition, long startingOffset) {
    FOR_THREAD
            .get()
            .positions
            .computeIfAbsent(new TopicPartition(topic, partition), ReadPosition::new)
            .startingOffset =
        startingOffset;
  }

  /** The positions of the partitions assigned to the reader on this thread. */
  public static List<ReadPosition> assignments() {
    return FOR_THREAD.get().positions.values().stream().filter(ReadPosition::isAssigned).toList();
  }

  /** Forgets this thread's positions, as a task's new thread starts without any. */
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

    private static final long UNASSIGNED = Long.MIN_VALUE;

    private final TopicPartition partition;

    /** The offset after the last consumed record. */
    private long nextOffset = -1L;

    /** Where the partition's split started, or {@link #UNASSIGNED} if this thread was not told. */
    private long startingOffset = UNASSIGNED;

    private ReadPosition(TopicPartition partition) {
      this.partition = partition;
    }

    public TopicPartition partition() {
      return partition;
    }

    public long nextOffset() {
      return nextOffset;
    }

    public long startingOffset() {
      return startingOffset;
    }

    /** Whether a record of the partition has been consumed on this thread. */
    public boolean isConsumed() {
      return nextOffset >= 0;
    }

    private boolean isAssigned() {
      return startingOffset != UNASSIGNED;
    }

    private boolean isFor(String topic, int partition) {
      return this.partition.partition() == partition && this.partition.topic().equals(topic);
    }
  }
}
