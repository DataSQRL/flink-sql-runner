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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.StringUtils;

public class DeserFailureHandlerOptions {

    public static final ConfigOption<DeserFailureHandlerType> SCAN_DESER_FAILURE_HANDLER =
            ConfigOptions.key("scan.deser-failure.handler")
                    .enumType(DeserFailureHandlerType.class)
                    .defaultValue(DeserFailureHandlerType.NONE);

    public static final ConfigOption<String> SCAN_DESER_FAILURE_TOPIC =
            ConfigOptions.key("scan.deser-failure.topic")
                    .stringType()
                    .noDefaultValue();

    public static void validateDeserFailureHandlerOptions(ReadableConfig tableOptions) {
        var handler = tableOptions.get(SCAN_DESER_FAILURE_HANDLER);
        var topic = tableOptions.get(SCAN_DESER_FAILURE_TOPIC);

        if (handler == DeserFailureHandlerType.KAFKA && StringUtils.isNullOrWhitespaceOnly(topic)) {
            throw new ValidationException(
                    String.format(
                            "'%s' is set to '%s', but '%s' is not specified.",
                            SCAN_DESER_FAILURE_HANDLER.key(),
                            DeserFailureHandlerType.KAFKA,
                            SCAN_DESER_FAILURE_TOPIC.key()));
        }

        if (handler != DeserFailureHandlerType.KAFKA && !StringUtils.isNullOrWhitespaceOnly(topic)) {
            throw new ValidationException(
                    String.format(
                            "'%s' is not set to '%s', but '%s' is specified.",
                            SCAN_DESER_FAILURE_HANDLER.key(),
                            DeserFailureHandlerType.KAFKA,
                            SCAN_DESER_FAILURE_TOPIC.key()));
        }
    }

    private DeserFailureHandlerOptions() {}
}
