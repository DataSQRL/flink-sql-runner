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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
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
    private long nextOffset = -1L;

    private ReadPosition(TopicPartition partition) {
      this.partition = partition;
    }

    public TopicPartition partition() {
      return partition;
    }

    public long nextOffset() {
      return nextOffset;
    }

    private boolean isFor(String topic, int partition) {
      return this.partition.partition() == partition && this.partition.topic().equals(topic);
    }
  }
}
