/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.table.DeserFailureHandlerOptions.SCAN_DESER_FAILURE_HANDLER;
import static org.apache.flink.streaming.connectors.kafka.table.DeserFailureHandlerOptions.SCAN_DESER_FAILURE_TOPIC;

public class DeserFailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DeserFailureHandler.class);

    private final DeserFailureHandlerType handlerType;
    private final @Nullable DeserFailureProducer producer;

    DeserFailureHandler(DeserFailureHandlerType handlerType, @Nullable DeserFailureProducer producer) {
        this.handlerType = handlerType;
        this.producer = producer;
    }

    static DeserFailureHandler of(ReadableConfig tableOptions, Properties consumerProps) {
        DeserFailureHandlerType handlerType = tableOptions.get(SCAN_DESER_FAILURE_HANDLER);

        DeserFailureProducer producer =
                handlerType == DeserFailureHandlerType.KAFKA
                        ? new DeserFailureProducer(tableOptions.get(SCAN_DESER_FAILURE_TOPIC), consumerProps)
                        : null;

        return new DeserFailureHandler(handlerType, producer);
    }

    void deserWithFailureHandling(ConsumerRecord<byte[], byte[]> record, DeserializationCaller deser)
            throws IOException {

        try {
            deser.call();
        } catch (IOException e) {
            if (DeserFailureHandlerType.NONE == handlerType) {
                throw e;

            } else if (DeserFailureHandlerType.LOG == handlerType) {
                LOG.info(
                        "Deserialization failure occurred for record. Topic: {}, Partition: {}, Offset: {}",
                        record.topic(),
                        record.partition(),
                        record.offset());

            } else if (DeserFailureHandlerType.KAFKA == handlerType) {
                LOG.info(
                        "Deserialization failure occurred for record, sending it to the configured topic ({}). Topic: {}, Partition: {}, Offset: {}",
                        producer.getTopic(),
                        record.topic(),
                        record.partition(),
                        record.offset());
                producer.send(record);
            }
        }
    }

    interface DeserializationCaller extends Serializable {
        void call() throws IOException;
    }
}
