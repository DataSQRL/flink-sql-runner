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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;

import com.datasqrl.flinkrunner.connector.kafka.DeserFailureHandler;
import com.datasqrl.flinkrunner.connector.kafka.DeserFailureHandlerOptions;
import com.datasqrl.flinkrunner.connector.kafka.DeserFailureHandlerType;
import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions;
import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SafeDynamicKafkaDeserializationSchema}. */
class SafeDynamicKafkaDeserializationSchemaTest {

    private final String topic = "topic-" + UUID.randomUUID();

    @Test
    void testRowsReachCollectorWhileTheirRecordIsLatest() throws Exception {
        var schema = createSchema(true);
        var positionsSeen = new ArrayList<ReadPosition>();

        schema.deserialize(record(42L, "row"), collector(positionsSeen));

        assertThat(positionsSeen).hasSize(1);
        assertThat(positionsSeen.get(0).partition()).isEqualTo(new TopicPartition(topic, 3));
        assertThat(positionsSeen.get(0).nextOffset()).isEqualTo(43L);
    }

    @Test
    void testRecordThatFailsToDeserializeStillMovesPosition() throws Exception {
        var schema = createSchema(true);
        var positionsSeen = new ArrayList<ReadPosition>();

        schema.deserialize(record(10L, "row"), collector(positionsSeen));
        schema.deserialize(record(11L, "not a row"), collector(positionsSeen));

        assertThat(positionsSeen).hasSize(1);
        assertThat(positionsSeen.get(0).nextOffset()).isEqualTo(12L);
    }

    @Test
    void testDoesNotTrackPositionsWithoutIdleAdvance() throws Exception {
        var schema = createSchema(false);
        var positionsSeen = new ArrayList<ReadPosition>();

        schema.deserialize(record(42L, "row"), collector(positionsSeen));

        // Whatever else this thread read last, it was not this record.
        assertThat(positionsSeen)
                .hasSize(1)
                .noneMatch(
                        position -> position != null && position.partition().topic().equals(topic));
    }

    private SafeDynamicKafkaDeserializationSchema createSchema(boolean trackReadPositions) {
        var tableOptions = new Configuration();
        tableOptions.set(
                DeserFailureHandlerOptions.SCAN_DESER_FAILURE_HANDLER, DeserFailureHandlerType.LOG);

        return new SafeDynamicKafkaDeserializationSchema(
                1,
                null,
                new int[0],
                new TextDeserializationSchema(),
                new int[] {0},
                false,
                new DynamicKafkaDeserializationSchema.MetadataConverter[0],
                TextDeserializationSchema.TYPE_INFO,
                false,
                DeserFailureHandler.of(tableOptions, new Properties()),
                trackReadPositions);
    }

    private ConsumerRecord<byte[], byte[]> record(long offset, String value) {
        return new ConsumerRecord<>(topic, 3, offset, null, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Collects the read position that is current when each row arrives. */
    private static Collector<RowData> collector(List<ReadPosition> positionsSeen) {
        return new Collector<>() {
            @Override
            public void collect(RowData row) {
                positionsSeen.add(KafkaReadPositions.latest());
            }

            @Override
            public void close() {}
        };
    }

    /** Turns a value into a one-column row, and fails for anything that is not {@code "row"}. */
    private static final class TextDeserializationSchema implements DeserializationSchema<RowData> {

        private static final TypeInformation<RowData> TYPE_INFO = TypeInformation.of(RowData.class);

        @Override
        public RowData deserialize(byte[] message) throws IOException {
            var text = new String(message, StandardCharsets.UTF_8);
            if (!text.equals("row")) {
                throw new IOException("Not a row: " + text);
            }
            return GenericRowData.of(StringData.fromString(text));
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TYPE_INFO;
        }
    }
}
