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

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeserFailureProducer implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DeserFailureProducer.class);
  private static final long serialVersionUID = 1L;

  private final String topic;
  private final Properties producerProps;

  private transient KafkaProducer<byte[], byte[]> producer;

  DeserFailureProducer(String topic, Properties consumerProps) {
    this.topic = checkNotNull(topic);

    producerProps = new Properties();
    producerProps.putAll(consumerProps);
    producerProps.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
  }

  private void init() {
    producer = new KafkaProducer<>(producerProps);
  }

  void send(ConsumerRecord<byte[], byte[]> record) {
    if (producer == null) {
      init();
    }

    if (record == null) {
      LOG.info("Unable to send deserialization failed record: Record was null.");
    } else {
      producer.send(
          new ProducerRecord<>(topic, null, null, record.key(), record.value(), record.headers()));
    }
  }

  public String getTopic() {
    return topic;
  }
}
