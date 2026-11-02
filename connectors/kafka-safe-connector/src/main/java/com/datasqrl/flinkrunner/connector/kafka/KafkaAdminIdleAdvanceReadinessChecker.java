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

  /** Includes {@code partition} in every later fetch. */
  public void register(TopicPartition partition) {
    brokerState().partitions.add(partition);
  }

  /**
   * The end of {@code partition}'s log as the latest fetch saw it, or null while that is unknown,
   * before a fetch since the partition was registered, or advancing is not safe.
   */
  @Nullable
  public LogEnd logEnd(TopicPartition partition, long currentTimeMillis) {
    var state = refreshIfDue(currentTimeMillis);
    return state.ready ? state.logEnds.get(partition) : null;
  }

  /**
   * The end of {@code position}'s partition's log as the latest fetch saw it, if {@code position}
   * has read to it; null while it has not, or while the end is unknown or advancing is not safe.
   */
  @Nullable
  public LogEnd logEndReachedBy(ReadPosition position, long currentTimeMillis) {
    var state = refreshIfDue(currentTimeMillis);
    return state.ready && isReadToEnd(state, position)
        ? state.logEnds.get(position.partition())
        : null;
  }

  /**
   * Whether every partition assigned to this subtask that has not produced a row has been read to
   * the end of its log, as the latest fetch saw it. A split that has not produced a row may be
   * marked idle only then: it does not know its partition yet, but it is one of these, and a split
   * marked idle with records left lets the subtask's other splits carry the watermark past them.
   */
  public boolean assignmentsWithoutRowsReadToEnd(long currentTimeMillis) {
    var state = refreshIfDue(currentTimeMillis);
    return state.ready
        && assignmentsWithoutRows(state).stream()
            .allMatch(position -> isReadToEnd(state, position));
  }

  /** Whether {@code position} has read to the end of its log, as the latest fetch saw it. */
  private boolean isReadToEnd(BrokerState state, ReadPosition position) {
    var logEnd = state.logEnds.get(position.partition());
    return logEnd != null && logEnd.isReachedBy(position, fetchPosition(position.partition()));
  }

  /**
   * The assigned partitions that have not produced a row, as a generator registers its partition
   * with its first row.
   */
  private static List<ReadPosition> assignmentsWithoutRows(BrokerState state) {
    return KafkaReadPositions.assignments().stream()
        .filter(position -> !state.partitions.contains(position.partition()))
        .toList();
  }

  /** How far the source's consumer has fetched {@code partition}, or null while it has not said. */
  @Nullable
  private FetchPosition fetchPosition(TopicPartition partition) {
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
    if (!state.ready) {
      return state;
    }

    var assignmentsWithoutRows = assignmentsWithoutRows(state);
    resolvePlaceholders(assignmentsWithoutRows);

    var partitions = new HashSet<>(state.partitions);
    assignmentsWithoutRows.forEach(position -> partitions.add(position.partition()));
    if (!partitions.isEmpty()) {
      // A failed listing keeps the previous log ends, each of which still holds for its own fetch.
      var logEnds = listLogEnds(partitions, currentTimeMillis);
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

  /**
   * Resolves where the splits of {@code positions} that start at a placeholder start reading
   * ({@link KafkaStartingOffsets}). What stays unresolved is tried again at the next refresh.
   */
  private void resolvePlaceholders(List<ReadPosition> positions) {
    var placeholders =
        positions.stream()
            .filter(ReadPosition::startsAtPlaceholder)
            .collect(Collectors.toMap(ReadPosition::partition, ReadPosition::startingOffset));
    if (placeholders.isEmpty()) {
      return;
    }

    Map<TopicPartition, Long> startingOffsets;
    try {
      startingOffsets =
          KafkaStartingOffsets.resolve(
              adminClient(), kafkaProperties, placeholders, brokerCheckTimeoutMillis);
    } catch (Exception e) {
      rethrowIfUnauthorized(e, "resolve starting offsets for idle watermark advance");
      log.debug("Failed to resolve starting offsets for idle watermark advance", e);
      return;
    }

    for (ReadPosition position : positions) {
      Long startingOffset = startingOffsets.get(position.partition());
      if (startingOffset != null) {
        position.resolveStartingOffset(startingOffset);
      }
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
     * Whether {@code position} has read every record this fetch saw. Neither the consumer nor the
     * split gets past an open transaction of a read_committed source, which therefore holds the
     * split back.
     *
     * @param fetched how far the source's consumer has fetched the partition, if it has said
     */
    boolean isReachedBy(ReadPosition position, @Nullable FetchPosition fetched) {
      // Offsets that hold no record, such as transaction markers, never reach the split, but the
      // consumer moves past them. Once the split has read every record the consumer returned,
      // even when that is none, it has read up to the consumer's position.
      long readUpTo =
          fetched != null && fetched.recordsEnd() == position.nextOffset()
              ? fetched.position()
              : position.nextOffset();
      // Records before where the split started were read before it.
      return Math.max(readUpTo, position.startingOffset()) >= highWatermark;
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
