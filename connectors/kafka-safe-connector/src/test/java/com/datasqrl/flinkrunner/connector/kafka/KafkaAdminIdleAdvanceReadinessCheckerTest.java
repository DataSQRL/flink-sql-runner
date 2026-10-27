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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.flinkrunner.connector.kafka.KafkaAdminIdleAdvanceReadinessChecker.LogEnd;
import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.flink.util.InstantiationUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaAdminIdleAdvanceReadinessCheckerTest {

  private static final long BROKER_CHECK_TTL_MILLIS = 10_000L;
  private static final TopicPartition PARTITION_0 = new TopicPartition("topic", 0);
  private static final TopicPartition PARTITION_1 = new TopicPartition("topic", 1);

  @Test
  void testCachesBrokerCheckUntilConfiguredTtlExpires() {
    var checker =
        new CountingReadinessChecker(
            new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 10_000));

    assertThat(checker.isReady(1000)).isTrue();
    assertThat(checker.isReady(10_999)).isTrue();
    assertThat(checker.checkCount).isEqualTo(1);

    assertThat(checker.isReady(11_000)).isFalse();
    assertThat(checker.checkCount).isEqualTo(2);
  }

  @Test
  void testThrowsWhenBrokerCheckFailsWithAuthorizationException() throws Exception {
    var checker = newReadinessChecker();
    var authorizationException = new AuthorizationException("describe configs denied");
    setAdminClient(checker, adminClientCompletingExceptionally(authorizationException));

    assertThatThrownBy(checker::checkBrokerAndTopicTimestampType)
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Cannot check idle watermark advance readiness: insufficient Kafka permissions.")
        .hasCause(authorizationException);
  }

  @Test
  void testReturnsFalseWhenBrokerCheckFailsWithoutAuthorizationException() throws Exception {
    var checker = newReadinessChecker();
    setAdminClient(
        checker, adminClientCompletingExceptionally(new RuntimeException("intermittent problem")));

    assertThat(checker.checkBrokerAndTopicTimestampType()).isFalse();
  }

  @Test
  void testFetchesLogEndsOfAllPartitionsAskedForTogether() throws Exception {
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    admin.highWatermark(PARTITION_1, 20L);
    var checker = newReadinessChecker(admin);

    assertThat(checker.logEnd(PARTITION_0, 1000L)).isEqualTo(new LogEnd(1000L, 10L));
    // The second partition joins the next fetch, which the TTL holds back.
    assertThat(checker.logEnd(PARTITION_1, 1000L)).isNull();

    long nextFetchMillis = 1000L + BROKER_CHECK_TTL_MILLIS;

    assertThat(checker.logEnd(PARTITION_1, nextFetchMillis))
        .isEqualTo(new LogEnd(nextFetchMillis, 20L));
    assertThat(checker.logEnd(PARTITION_0, nextFetchMillis))
        .isEqualTo(new LogEnd(nextFetchMillis, 10L));
    assertThat(admin.listOffsetsCalls)
        .containsExactly(
            new ListOffsetsCall(Set.of(PARTITION_0), IsolationLevel.READ_UNCOMMITTED),
            new ListOffsetsCall(Set.of(PARTITION_0, PARTITION_1), IsolationLevel.READ_UNCOMMITTED));
  }

  @Test
  void testFetchesAgainOnlyOnceTtlIsUp() throws Exception {
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    var checker = newReadinessChecker(admin);

    checker.logEnd(PARTITION_0, 1000L);
    admin.highWatermark(PARTITION_0, 15L);

    assertThat(checker.logEnd(PARTITION_0, 1000L + BROKER_CHECK_TTL_MILLIS - 1))
        .isEqualTo(new LogEnd(1000L, 10L));
    assertThat(checker.logEnd(PARTITION_0, 1000L + BROKER_CHECK_TTL_MILLIS))
        .isEqualTo(new LogEnd(1000L + BROKER_CHECK_TTL_MILLIS, 15L));
    assertThat(admin.listOffsetsCalls).hasSize(2);
  }

  @Test
  void testHasNoLogEndWhileTopicIsNotLogAppendTime() throws Exception {
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    admin.logAppendTime = false;
    var checker = newReadinessChecker(admin);

    assertThat(checker.logEnd(PARTITION_0, 1000L)).isNull();
    assertThat(admin.listOffsetsCalls).isEmpty();
  }

  @Test
  void testWithholdsKnownLogEndOnceTopicStopsBeingLogAppendTime() throws Exception {
    // Records written from then on carry producer timestamps, which the log end says nothing about.
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    var checker = newReadinessChecker(admin);
    checker.logEnd(PARTITION_0, 1000L);

    admin.logAppendTime = false;

    assertThat(checker.logEnd(PARTITION_0, 1000L + BROKER_CHECK_TTL_MILLIS)).isNull();
  }

  @Test
  void testFailedFetchKeepsPreviousLogEnd() throws Exception {
    // It still holds for the moment it was fetched, and the generator goes no further than that.
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    var checker = newReadinessChecker(admin);
    checker.logEnd(PARTITION_0, 1000L);

    admin.listOffsetsFailure = new RuntimeException("broker unavailable");

    assertThat(checker.logEnd(PARTITION_0, 1000L + BROKER_CHECK_TTL_MILLIS))
        .isEqualTo(new LogEnd(1000L, 10L));
    assertThat(admin.listOffsetsCalls).hasSize(2);
  }

  @Test
  void testHasNoLogEndUntilAFetchSucceeds() throws Exception {
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    admin.listOffsetsFailure = new RuntimeException("broker unavailable");
    var checker = newReadinessChecker(admin);

    assertThat(checker.logEnd(PARTITION_0, 1000L)).isNull();

    admin.listOffsetsFailure = null;

    assertThat(checker.logEnd(PARTITION_0, 1000L + BROKER_CHECK_TTL_MILLIS))
        .isEqualTo(new LogEnd(1000L + BROKER_CHECK_TTL_MILLIS, 10L));
  }

  @Test
  void testThrowsWhenListingOffsetsFailsWithAuthorizationException() throws Exception {
    var admin = new FakeAdmin();
    var authorizationException = new AuthorizationException("describe topic denied");
    admin.listOffsetsFailure = authorizationException;
    var checker = newReadinessChecker(admin);

    assertThatThrownBy(() -> checker.logEnd(PARTITION_0, 1000L))
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Cannot list log end offsets for idle watermark advance: insufficient Kafka permissions.")
        .hasCause(authorizationException);
  }

  @Test
  void testTracksLogEndsAfterJavaSerialization() throws Exception {
    // Flink ships the checker to each subtask serialized; its transient state starts out empty.
    var admin = new FakeAdmin();
    admin.highWatermark(PARTITION_0, 10L);
    var checker = InstantiationUtil.clone(newReadinessChecker());
    setAdminClient(checker, admin.adminClient());

    assertThat(checker.logEnd(PARTITION_0, 1000L)).isEqualTo(new LogEnd(1000L, 10L));
  }

  private static KafkaAdminIdleAdvanceReadinessChecker newReadinessChecker() {
    return new KafkaAdminIdleAdvanceReadinessChecker(
        new Properties(),
        List.of("topic"),
        new SourceWatermarkConfig(
            250, 50, 1000, 0.95D, 1000, 10_000, 1000, BROKER_CHECK_TTL_MILLIS));
  }

  private static KafkaAdminIdleAdvanceReadinessChecker newReadinessChecker(FakeAdmin admin)
      throws Exception {
    var checker = newReadinessChecker();
    setAdminClient(checker, admin.adminClient());
    return checker;
  }

  private static AdminClient adminClientCompletingExceptionally(Exception exception) {
    var adminClient = mock(AdminClient.class);
    var describeConfigsResult = mock(DescribeConfigsResult.class);
    var future = new KafkaFutureImpl<Map<ConfigResource, Config>>();
    future.completeExceptionally(exception);

    when(adminClient.describeConfigs(anyCollection())).thenReturn(describeConfigsResult);
    when(describeConfigsResult.all()).thenReturn(future);

    return adminClient;
  }

  private static void setAdminClient(
      KafkaAdminIdleAdvanceReadinessChecker checker, AdminClient adminClient) throws Exception {

    var adminClientField =
        KafkaAdminIdleAdvanceReadinessChecker.class.getDeclaredField("adminClient");
    adminClientField.setAccessible(true);
    adminClientField.set(checker, adminClient);
  }

  private record ListOffsetsCall(Set<TopicPartition> partitions, IsolationLevel isolationLevel) {}

  /**
   * An AdminClient that reports the topic timestamp type and the high watermarks set, and records
   * every {@code listOffsets} call.
   */
  private static final class FakeAdmin {

    private final Map<TopicPartition, Long> highWatermarks = new HashMap<>();
    private final List<ListOffsetsCall> listOffsetsCalls = new ArrayList<>();
    private boolean logAppendTime = true;
    private Exception listOffsetsFailure;

    private void highWatermark(TopicPartition partition, long offset) {
      highWatermarks.put(partition, offset);
    }

    private AdminClient adminClient() {
      var adminClient = mock(AdminClient.class);
      when(adminClient.describeConfigs(anyCollection()))
          .thenAnswer(invocation -> describeConfigs(invocation.getArgument(0)));
      when(adminClient.listOffsets(anyMap(), any(ListOffsetsOptions.class)))
          .thenAnswer(
              invocation -> listOffsets(invocation.getArgument(0), invocation.getArgument(1)));
      return adminClient;
    }

    private DescribeConfigsResult describeConfigs(Collection<ConfigResource> topics) {
      var timestampType = logAppendTime ? TimestampType.LOG_APPEND_TIME : TimestampType.CREATE_TIME;
      var config =
          new Config(
              List.of(
                  new ConfigEntry(
                      TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timestampType.toString())));
      var configs = new HashMap<ConfigResource, Config>();
      topics.forEach(topic -> configs.put(topic, config));

      var result = mock(DescribeConfigsResult.class);
      when(result.all()).thenReturn(KafkaFuture.completedFuture(configs));
      return result;
    }

    private ListOffsetsResult listOffsets(
        Map<TopicPartition, OffsetSpec> request, ListOffsetsOptions options) {
      listOffsetsCalls.add(
          new ListOffsetsCall(Set.copyOf(request.keySet()), options.isolationLevel()));

      var futures = new HashMap<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>();
      for (TopicPartition partition : request.keySet()) {
        var future = new KafkaFutureImpl<ListOffsetsResultInfo>();
        if (listOffsetsFailure != null) {
          future.completeExceptionally(listOffsetsFailure);
        } else {
          long offset = highWatermarks.get(partition);
          future.complete(new ListOffsetsResultInfo(offset, -1L, Optional.empty()));
        }
        futures.put(partition, future);
      }

      return new ListOffsetsResult(futures);
    }
  }

  private static final class CountingReadinessChecker
      extends KafkaAdminIdleAdvanceReadinessChecker {

    private int checkCount;

    private CountingReadinessChecker(SourceWatermarkConfig config) {
      super(new Properties(), List.of("topic"), config);
    }

    @Override
    boolean checkBrokerAndTopicTimestampType() {
      checkCount++;
      return checkCount == 1;
    }
  }
}
