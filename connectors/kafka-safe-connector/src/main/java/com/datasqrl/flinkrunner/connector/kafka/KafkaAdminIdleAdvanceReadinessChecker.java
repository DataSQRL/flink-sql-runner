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

import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.io.Serial;
import java.io.Serializable;
import java.lang.ref.Cleaner;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.record.TimestampType;

/** Readiness checker for idle source-watermark advancement backed by Kafka AdminClient metadata. */
@Slf4j
public class KafkaAdminIdleAdvanceReadinessChecker implements IdleAdvanceReadinessChecker {

  @Serial private static final long serialVersionUID = 1L;

  private static final Cleaner ADMIN_CLIENT_CLEANER = Cleaner.create();

  private final Properties kafkaProperties;
  private final List<String> topics;
  private final long brokerCheckTimeoutMillis;
  private final long brokerCheckTtlMillis;

  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private transient Cleaner.Cleanable adminClientCleanable;

  private transient AdminClient adminClient;
  private transient long lastCheckMillis = Long.MIN_VALUE;
  private transient boolean lastCheckReady;

  public KafkaAdminIdleAdvanceReadinessChecker(
      Properties kafkaProperties, List<String> topics, SourceWatermarkConfig config) {
    checkArgument(
        topics != null && !topics.isEmpty(),
        "Watermark idle advance only supports the 'topic' configuration");
    this.kafkaProperties = kafkaProperties;
    this.topics = List.copyOf(topics);
    this.brokerCheckTimeoutMillis = config.idleAdvanceBrokerCheckTimeoutMillis();
    this.brokerCheckTtlMillis = config.idleAdvanceBrokerCheckTtlMillis();
  }

  /**
   * Returns whether it is safe to emit wall-clock-derived idle source watermarks.
   *
   * <p>Idle watermark advancement uses wall-clock time to move event time forward while no records
   * are arriving. That is only safe when the broker is reachable and the source topics use Kafka
   * {@code LogAppendTime}.
   */
  @Override
  public boolean isReady(long currentTimeMillis) {
    if (lastCheckMillis != Long.MIN_VALUE
        && currentTimeMillis - lastCheckMillis < brokerCheckTtlMillis) {
      return lastCheckReady;
    }

    lastCheckMillis = currentTimeMillis;
    lastCheckReady = checkBrokerAndTopicTimestampType();

    return lastCheckReady;
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
    } catch (AuthorizationException e) {
      throw new RuntimeException(
          "Cannot check idle watermark advance readiness: insufficient Kafka permissions.", e);

    } catch (Exception e) {
      log.debug("Failed to check idle watermark advance readiness", e);
      return false;
    }
  }

  private AdminClient adminClient() {
    if (adminClient == null) {
      adminClient = AdminClient.create(kafkaProperties);
      adminClientCleanable = ADMIN_CLIENT_CLEANER.register(this, adminClient::close);
    }

    return adminClient;
  }

  private List<ConfigResource> mapTopicsToConfigResources() {
    return topics.stream()
        .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
        .toList();
  }
}

/** Determines whether it is currently safe to emit wall-clock-derived idle source watermarks. */
@FunctionalInterface
interface IdleAdvanceReadinessChecker extends Serializable {

  boolean isReady(long currentTimeMillis);
}
