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

import com.datasqrl.flinkrunner.connector.kafka.KafkaFetchPositions.FetchPosition;
import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.io.Serial;
import java.io.Serializable;
import java.lang.ref.Cleaner;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.record.TimestampType;

/**
 * Asks the Kafka brokers what idle source-watermark advancement needs to know: whether it is safe
 * at all, and how far each partition's log reaches.
 *
 * <p>Flink creates one per source subtask, shared by the watermark generators of all its splits and
 * called from the subtask's task thread only. Every broker-check TTL it asks the brokers once, for
 * all partitions at the same time.
 */
@Slf4j
public class KafkaAdminIdleAdvanceReadinessChecker implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private static final Cleaner ADMIN_CLIENT_CLEANER = Cleaner.create();

  private final Properties kafkaProperties;
  private final List<String> topics;
  private final long brokerCheckTimeoutMillis;
  private final long brokerCheckTtlMillis;

  /** The name under which the source's consumer publishes its {@link KafkaFetchPositions}. */
  @Nullable private final String sourceId;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private transient Cleaner.Cleanable adminClientCleanable;

  private transient AdminClient adminClient;
  private transient BrokerState brokerState;

  public KafkaAdminIdleAdvanceReadinessChecker(
      Properties kafkaProperties, List<String> topics, SourceWatermarkConfig config) {
    checkArgument(
        topics != null && !topics.isEmpty(),
        "Watermark idle advance only supports the 'topic' configuration");
    this.kafkaProperties = kafkaProperties;
    this.topics = List.copyOf(topics);
    this.brokerCheckTimeoutMillis = config.idleAdvanceBrokerCheckTimeoutMillis();
    this.brokerCheckTtlMillis = config.idleAdvanceBrokerCheckTtlMillis();
    this.sourceId = kafkaProperties.getProperty(KafkaFetchPositions.SOURCE_ID_CONFIG);
  }

  /**
   * Returns whether it is safe to emit wall-clock-derived idle source watermarks.
   *
   * <p>Idle watermark advancement uses wall-clock time to move event time forward while partitions
   * are idle. That is only safe when the broker is reachable and the source topics use Kafka {@code
   * LogAppendTime}.
   */
  public boolean isReady(long currentTimeMillis) {
    return refreshIfDue(currentTimeMillis).ready;
  }

  /**
   * The end of {@code partition}'s log as the latest fetch saw it, or null while that is unknown or
   * advancing is not safe. Asking for a partition includes it in every later fetch, so the first
   * answer may take up to one broker-check TTL.
   */
  @Nullable
  public LogEnd logEnd(TopicPartition partition, long currentTimeMillis) {
    brokerState().partitions.add(partition);
    var state = refreshIfDue(currentTimeMillis);
    return state.ready ? state.logEnds.get(partition) : null;
  }

  /** How far the source's consumer has fetched {@code partition}, or null while it has not said. */
  @Nullable
  public FetchPosition fetchPosition(TopicPartition partition) {
    return sourceId == null ? null : KafkaFetchPositions.fetchPosition(sourceId, partition);
  }

  private BrokerState refreshIfDue(long currentTimeMillis) {
    var state = brokerState();
    if (state.refreshed && currentTimeMillis - state.refreshedAtMillis < brokerCheckTtlMillis) {
      return state;
    }

    state.refreshed = true;
    state.refreshedAtMillis = currentTimeMillis;
    state.ready = checkBrokerAndTopicTimestampType();
    if (state.ready && !state.partitions.isEmpty()) {
      // A failed listing keeps the previous log ends, each of which still holds for its own fetch.
      var logEnds = listLogEnds(Set.copyOf(state.partitions), currentTimeMillis);
      if (logEnds != null) {
        state.logEnds.putAll(logEnds);
      }
    }

    return state;
  }

  /**
   * Lists the end of each partition's log, as of a fetch issued at {@code requestedAtMillis}.
   * Returns null if the broker cannot be asked.
   */
  @Nullable
  Map<TopicPartition, LogEnd> listLogEnds(Set<TopicPartition> partitions, long requestedAtMillis) {
    var request =
        partitions.stream()
            .collect(Collectors.toMap(Function.identity(), p -> OffsetSpec.latest()));

    try {
      return adminClient()
          .listOffsets(request, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED))
          .all()
          .get(brokerCheckTimeoutMillis, TimeUnit.MILLISECONDS)
          .entrySet()
          .stream()
          .collect(
              Collectors.toMap(
                  Map.Entry::getKey, e -> new LogEnd(requestedAtMillis, e.getValue().offset())));
    } catch (Exception e) {
      rethrowIfUnauthorized(e, "list log end offsets for idle watermark advance");
      log.debug("Failed to list log end offsets for idle watermark advance", e);
      return null;
    }
  }

  boolean checkBrokerAndTopicTimestampType() {
    try {
      var configs =
          adminClient()
              .describeConfigs(mapTopicsToConfigResources())
              .all()
              .get(brokerCheckTimeoutMillis, TimeUnit.MILLISECONDS);

      return configs.values().stream()
          .allMatch(
              config -> {
                var timestampType = config.get(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG);
                return TimestampType.LOG_APPEND_TIME.toString().equals(timestampType.value());
              });
    } catch (Exception e) {
      rethrowIfUnauthorized(e, "check idle watermark advance readiness");
      log.debug("Failed to check idle watermark advance readiness", e);
      return false;
    }
  }

  /** Fails the job on missing Kafka permissions, which no retry fixes. */
  private static void rethrowIfUnauthorized(Exception e, String action) {
    if (e.getCause() instanceof AuthorizationException ae) {
      throw new RuntimeException(
          "Cannot %s: insufficient Kafka permissions.".formatted(action), ae);
    }
  }

  private AdminClient adminClient() {
    if (adminClient == null) {
      adminClient = AdminClient.create(kafkaProperties);
      adminClientCleanable = ADMIN_CLIENT_CLEANER.register(this, adminClient::close);
    }

    return adminClient;
  }

  private BrokerState brokerState() {
    if (brokerState == null) {
      brokerState = new BrokerState();
    }

    return brokerState;
  }

  private List<ConfigResource> mapTopicsToConfigResources() {
    return topics.stream()
        .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
        .toList();
  }

  /**
   * The end of a partition's log as one fetch saw it.
   *
   * @param requestedAtMillis the wall-clock time at which the fetch was issued
   * @param highWatermark the offset after the last record in the log
   */
  public record LogEnd(long requestedAtMillis, long highWatermark) {

    /**
     * Whether {@code position} has read every record this fetch saw.
     *
     * @param fetched how far the source's consumer has fetched the partition, if it has said
     */
    boolean isReachedBy(ReadPosition position, @Nullable FetchPosition fetched) {
      // Offsets that hold no record, such as transaction markers, never reach the split. The
      // consumer moves past them, so once the split has read every record the consumer returned,
      // it has read up to the consumer's position. Neither gets past an open transaction of a
      // read_committed source, which therefore holds the split back.
      long readUpTo =
          fetched != null && fetched.recordsEnd() == position.nextOffset()
              ? fetched.position()
              : position.nextOffset();
      return readUpTo >= highWatermark;
    }
  }

  /** Created after deserialization, since transient fields do not keep their initializers. */
  private static final class BrokerState {

    private final Set<TopicPartition> partitions = new HashSet<>();
    private final Map<TopicPartition, LogEnd> logEnds = new HashMap<>();
    private boolean refreshed;
    private long refreshedAtMillis;
    private boolean ready;
  }
}
