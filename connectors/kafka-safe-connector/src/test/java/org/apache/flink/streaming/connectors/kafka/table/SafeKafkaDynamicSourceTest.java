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

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.BoundedMode;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;

import com.datasqrl.flinkrunner.connector.kafka.DeserFailureHandler;
import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests when {@link SafeKafkaDynamicSource} decodes the Kafka record key. */
class SafeKafkaDynamicSourceTest {

    private static final DataType PHYSICAL_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("id", DataTypes.BIGINT()),
                    DataTypes.FIELD("name", DataTypes.STRING()));

    private static final int[] KEY_FIELDS = {0};
    private static final int[] ALL_FIELDS = {0, 1};
    private static final int[] NON_KEY_FIELDS = {1};

    private static final GenericRowData ROW = GenericRowData.of(42L, StringData.fromString("Ada"));

    @Test
    void shouldReadRecordWithoutKeyWhenValueContainsKeyFields() throws Exception {
        var schema = createSchema(ALL_FIELDS, false);

        assertThat(deserialize(schema, null, "42,Ada")).containsExactly(ROW);
    }

    @Test
    void shouldReadRecordWithKeyWhenValueContainsKeyFields() throws Exception {
        var schema = createSchema(ALL_FIELDS, false);

        assertThat(deserialize(schema, "42", "42,Ada")).containsExactly(ROW);
    }

    @Test
    void shouldTakeKeyFieldsFromKeyWhenValueExcludesThem() throws Exception {
        var schema = createSchema(NON_KEY_FIELDS, false);

        assertThat(deserialize(schema, "42", "Ada")).containsExactly(ROW);
    }

    @Test
    void shouldStillDecodeKeyInUpsertMode() throws Exception {
        var schema = createSchema(ALL_FIELDS, true);

        assertThat(deserialize(schema, null, "42,Ada")).isEmpty();
        assertThat(deserialize(schema, "42", "42,Ada")).containsExactly(ROW);
    }

    @Test
    void shouldDropRecordWithoutKeyWhenReadingKeyFieldsFromValueIsDisabled() throws Exception {
        var schema = createSchema(ALL_FIELDS, false, false);

        assertThat(deserialize(schema, null, "42,Ada")).isEmpty();
        assertThat(deserialize(schema, "42", "42,Ada")).containsExactly(ROW);
    }

    @Test
    void shouldReadValueFormatMetadataWhenValueContainsKeyFields() throws Exception {
        var schema = createSchema(ALL_FIELDS, false, false, List.of("value.m"));

        assertThat(deserialize(schema, "42", "42,Ada,m"))
                .containsExactly(
                        GenericRowData.of(
                                42L, StringData.fromString("Ada"), StringData.fromString("m")));
    }

    private static KafkaRecordDeserializationSchema<RowData> createSchema(
            int[] valueProjection, boolean upsertMode) {
        return createSchema(valueProjection, upsertMode, true);
    }

    private static KafkaRecordDeserializationSchema<RowData> createSchema(
            int[] valueProjection, boolean upsertMode, boolean keyFieldsFromValue) {
        return createSchema(valueProjection, upsertMode, keyFieldsFromValue, List.of());
    }

    @SuppressWarnings("unchecked")
    private static KafkaRecordDeserializationSchema<RowData> createSchema(
            int[] valueProjection,
            boolean upsertMode,
            boolean keyFieldsFromValue,
            List<String> formatMetadataKeys) {
        var source =
                new SafeKafkaDynamicSource(
                        PHYSICAL_TYPE,
                        null,
                        mock(DecodingFormat.class),
                        KEY_FIELDS,
                        valueProjection,
                        null,
                        List.of("topic"),
                        null,
                        new Properties(),
                        StartupMode.EARLIEST,
                        Map.of(),
                        0L,
                        BoundedMode.UNBOUNDED,
                        Map.of(),
                        0L,
                        upsertMode,
                        keyFieldsFromValue,
                        "test",
                        null,
                        DeserFailureHandler.of(new Configuration(), new Properties()),
                        Optional.empty(),
                        WatermarkEmitStrategy.ON_EVENT,
                        Optional.empty(),
                        new SourceWatermarkConfig());

        var producedFields = new ArrayList<>(DataType.getFields(PHYSICAL_TYPE));
        formatMetadataKeys.forEach(
                key -> producedFields.add(DataTypes.FIELD(key, DataTypes.STRING())));
        source.applyReadableMetadata(
                formatMetadataKeys, DataTypes.ROW(producedFields.toArray(DataTypes.Field[]::new)));

        var keyDeserialization =
                new CsvDeserializationSchema(
                        fields -> GenericRowData.of(Long.parseLong(fields[0])));
        var valueDeserialization =
                new CsvDeserializationSchema(
                        fields ->
                                switch (fields.length) {
                                    case 1 -> GenericRowData.of(StringData.fromString(fields[0]));
                                    case 2 -> GenericRowData.of(
                                            Long.parseLong(fields[0]),
                                            StringData.fromString(fields[1]));
                                    default -> GenericRowData.of(
                                            Long.parseLong(fields[0]),
                                            StringData.fromString(fields[1]),
                                            StringData.fromString(fields[2]));
                                });

        return source.createKafkaDeserializationSchema(
                keyDeserialization, valueDeserialization, mock(TypeInformation.class));
    }

    private static List<RowData> deserialize(
            KafkaRecordDeserializationSchema<RowData> schema, @Nullable String key, String value)
            throws Exception {
        var record = new ConsumerRecord<>("topic", 0, 0L, bytes(key), bytes(value));
        var rows = new ArrayList<RowData>();
        schema.deserialize(record, new ListCollector<>(rows));
        return rows;
    }

    private static byte[] bytes(@Nullable String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    /** Decodes comma-separated fields and, like the JSON formats, emits nothing for null. */
    private static class CsvDeserializationSchema implements DeserializationSchema<RowData> {

        private final Function<String[], RowData> converter;

        CsvDeserializationSchema(Function<String[], RowData> converter) {
            this.converter = converter;
        }

        @Override
        public RowData deserialize(byte[] message) {
            return message == null
                    ? null
                    : converter.apply(new String(message, StandardCharsets.UTF_8).split(","));
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return mock(TypeInformation.class);
        }
    }
}
