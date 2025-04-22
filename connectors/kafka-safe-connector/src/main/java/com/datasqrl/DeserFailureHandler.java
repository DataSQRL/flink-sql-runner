/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl;

import static com.datasqrl.DeserFailureHandlerOptions.SCAN_DESER_FAILURE_HANDLER;
import static com.datasqrl.DeserFailureHandlerOptions.SCAN_DESER_FAILURE_TOPIC;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeserFailureHandler implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DeserFailureHandler.class);
  private static final long serialVersionUID = 1L;

  private final DeserFailureHandlerType handlerType;
  private final @Nullable DeserFailureProducer producer;

  DeserFailureHandler(
      DeserFailureHandlerType handlerType, @Nullable DeserFailureProducer producer) {
    this.handlerType = handlerType;
    this.producer = producer;
  }

  public static DeserFailureHandler of(ReadableConfig tableOptions, Properties consumerProps) {
    DeserFailureHandlerType handlerType = tableOptions.get(SCAN_DESER_FAILURE_HANDLER);

    DeserFailureProducer producer =
        handlerType == DeserFailureHandlerType.KAFKA
            ? new DeserFailureProducer(tableOptions.get(SCAN_DESER_FAILURE_TOPIC), consumerProps)
            : null;

    return new DeserFailureHandler(handlerType, producer);
  }

  public void deserWithFailureHandling(
      ConsumerRecord<byte[], byte[]> record, DeserializationCaller deser) throws Exception {

    try {
      deser.call();
    } catch (IOException e) {
      if (DeserFailureHandlerType.NONE == handlerType) {
        throw e;

      } else if (DeserFailureHandlerType.LOG == handlerType) {
        LOG.warn(
            "Deserialization failure occurred. Topic: {}, Partition: {}, Offset: {}",
            record.topic(),
            record.partition(),
            record.offset());

      } else if (DeserFailureHandlerType.KAFKA == handlerType) {
        LOG.warn(
            "Deserialization failure occurred, sending the record to the configured topic ({}). Topic: {}, Partition: {}, Offset: {}",
            producer.getTopic(),
            record.topic(),
            record.partition(),
            record.offset());
        producer.send(record);
      }

      LOG.debug("Failure cause", e);
      LOG.trace("Failed record: {}", record);
    }
  }

  public interface DeserializationCaller extends Serializable {
    void call() throws Exception;
  }
}
