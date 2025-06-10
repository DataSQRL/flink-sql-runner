/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.datasqrl.flinkrunner.connector.kafka.DeserFailureHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

/** A specific {@link KafkaSerializationSchema} for {@link SafeKafkaDynamicSource}. */
public class SafeDynamicKafkaDeserializationSchema extends DynamicKafkaDeserializationSchema {

    private final DeserFailureHandler deserFailureHandler;

    SafeDynamicKafkaDeserializationSchema(
            int physicalArity,
            @Nullable DeserializationSchema<RowData> keyDeserialization,
            int[] keyProjection,
            DeserializationSchema<RowData> valueDeserialization,
            int[] valueProjection,
            boolean hasMetadata,
            MetadataConverter[] metadataConverters,
            TypeInformation<RowData> producedTypeInfo,
            boolean upsertMode,
            DeserFailureHandler deserFailureHandler) {
        super(
                physicalArity,
                keyDeserialization,
                keyProjection,
                valueDeserialization,
                valueProjection,
                hasMetadata,
                metadataConverters,
                producedTypeInfo,
                upsertMode);
        this.deserFailureHandler = deserFailureHandler;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws Exception {
        deserFailureHandler.deserWithFailureHandling(
                record, () -> super.deserialize(record, collector));
    }
}
