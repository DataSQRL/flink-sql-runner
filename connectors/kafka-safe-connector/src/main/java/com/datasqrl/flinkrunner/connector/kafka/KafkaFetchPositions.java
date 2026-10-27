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

import static org.apache.flink.util.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * How far the Kafka source's consumer has fetched each partition, which a consumer interceptor
 * learns from every poll.
 *
 * <p>The consumer moves its position past offsets that hold no record it returns: transaction
 * markers, and under {@code read_committed} the records of aborted transactions. From offsets
 * alone, a split cannot tell those from records it has yet to read. Every poll hands its
 * interceptors the positions it moved to, also a poll that only moved past such offsets ({@link
 * ConsumerRecords#nextOffsets()}). The interceptor runs on the source's fetcher thread, ahead of
 * the task thread that reads the records, so it publishes each position together with the end of
 * the records the consumer returned before it: a split that has read those has read everything
 * below the position.
 *
 * <p>Kafka creates the interceptor from the consumer's properties, so the source names itself there
 * ({@link #publishing}) and its {@link KafkaAdminIdleAdvanceReadinessChecker} looks the positions
 * up under that name.
 */
@Internal
public final class KafkaFetchPositions implements ConsumerInterceptor<byte[], byte[]> {

  /** Consumer property naming the source whose positions the interceptor publishes. */
  public static final String SOURCE_ID_CONFIG = "datasqrl.fetch-positions.source-id";

  private static final Map<Key, FetchPosition> POSITIONS = new ConcurrentHashMap<>();

  private String sourceId;

  /** The position this consumer published last, per partition. */
  private final Map<TopicPartition, FetchPosition> published = new HashMap<>();

  /**
   * Returns a copy of {@code properties} for a consumer that publishes its positions for the source
   * {@code sourceId}.
   */
  public static Properties publishing(Properties properties, String sourceId) {
    var publishing = new Properties();
    publishing.putAll(properties);

    String interceptors =
        properties.getProperty(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "").trim();
    String interceptor = KafkaFetchPositions.class.getName();
    publishing.setProperty(
        ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
        interceptors.isEmpty() ? interceptor : interceptors + "," + interceptor);
    publishing.setProperty(SOURCE_ID_CONFIG, sourceId);
    return publishing;
  }

  /**
   * How far the consumer of the source {@code sourceId} has fetched {@code partition}, or null
   * before it has returned a record from it.
   */
  @Nullable
  public static FetchPosition fetchPosition(String sourceId, TopicPartition partition) {
    return POSITIONS.get(new Key(sourceId, partition));
  }

  @Override
  public void configure(Map<String, ?> configs) {
    Object configured = configs.get(SOURCE_ID_CONFIG);
    checkArgument(
        configured != null, "%s needs the consumer property %s", getClass(), SOURCE_ID_CONFIG);
    sourceId = configured.toString();
  }

  @Override
  public ConsumerRecords<byte[], byte[]> onConsume(ConsumerRecords<byte[], byte[]> records) {
    records
        .nextOffsets()
        .forEach(
            (partition, next) -> {
              var partitionRecords = records.records(partition);
              FetchPosition previous = published.get(partition);
              long recordsEnd;
              if (!partitionRecords.isEmpty()) {
                recordsEnd = partitionRecords.get(partitionRecords.size() - 1).offset() + 1;
              } else if (previous != null) {
                recordsEnd = previous.recordsEnd();
              } else {
                // Nothing ties offsets passed before the first record to a split's reads.
                return;
              }

              var position = new FetchPosition(recordsEnd, next.offset());
              published.put(partition, position);
              POSITIONS.put(new Key(sourceId, partition), position);
            });

    return records;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {}

  @Override
  public void close() {
    // After a failover, the consumer that reads a partition now may have published since.
    published.forEach(
        (partition, position) ->
            POSITIONS.computeIfPresent(
                new Key(sourceId, partition),
                (key, current) -> current == position ? null : current));
  }

  /**
   * How far the consumer has fetched a partition.
   *
   * @param recordsEnd the offset after the last record the consumer returned
   * @param position the consumer's position, which it reached without returning a record after
   *     {@code recordsEnd}
   */
  public record FetchPosition(long recordsEnd, long position) {}

  private record Key(String sourceId, TopicPartition partition) {}
}
