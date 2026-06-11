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
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

class KafkaAdminIdleAdvanceReadinessCheckerTest {

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

  private static KafkaAdminIdleAdvanceReadinessChecker newReadinessChecker() {
    return new KafkaAdminIdleAdvanceReadinessChecker(
        new Properties(),
        List.of("topic"),
        new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 10_000));
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
