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
package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import com.datasqrl.flinkrunner.connector.kafka.DeserFailureHandler;
import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.io.IOException;

/** A specific {@link KafkaRecordDeserializationSchema} for {@link SafeKafkaDynamicSource}. */
public class SafeDynamicKafkaDeserializationSchema extends DynamicKafkaDeserializationSchema {

    private final DeserFailureHandler deserFailureHandler;

    /**
     * Whether to record each consumed record in {@link KafkaReadPositions}, for the idle
     * advancement of {@code KafkaRecordTimestampWatermarkStrategy}.
     */
    private final boolean trackReadPositions;

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
            DeserFailureHandler deserFailureHandler,
            boolean trackReadPositions) {
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
        this.trackReadPositions = trackReadPositions;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector)
            throws IOException {
        if (trackReadPositions) {
            // Before deserializing, so a record that produces no row still moves the position.
            KafkaReadPositions.consumed(record.topic(), record.partition(), record.offset());
        }

        deserFailureHandler.deserWithFailureHandling(
                record, () -> super.deserialize(record, collector));
    }
}
