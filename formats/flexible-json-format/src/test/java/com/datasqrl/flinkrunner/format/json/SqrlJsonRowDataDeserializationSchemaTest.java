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
package com.datasqrl.flinkrunner.format.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class SqrlJsonRowDataDeserializationSchemaTest {

  @Test
  void decodingFormatUsesBuiltinJsonDefaults() throws Exception {
    var schema = createDecoder(new Configuration(), DataTypes.TIMESTAMP(3));

    var rows = deserialize(schema, "{\"value\":\"2026-09-23 15:30:45\"}");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getTimestamp(0, 3))
        .isEqualTo(TimestampData.fromTimestamp(java.sql.Timestamp.valueOf("2026-09-23 15:30:45")));
  }

  @Test
  void decodingFormatUsesConfiguredErrorHandlingAndTimestampFormat() throws Exception {
    var options = new Configuration();
    options.set(JsonFormatOptions.FAIL_ON_MISSING_FIELD, true);
    options.set(JsonFormatOptions.TIMESTAMP_FORMAT, "ISO-8601");
    var schema = createDecoder(options, DataTypes.TIMESTAMP(3));

    var rows = deserialize(schema, "{\"value\":\"2026-09-23T15:30:45\"}");

    assertThat(rows).hasSize(1);
    assertThatThrownBy(() -> deserialize(schema, "{}")).isInstanceOf(java.io.IOException.class);
  }

  @Test
  void decodingFormatIgnoresParseErrorsWhenConfigured() throws Exception {
    var options = new Configuration();
    options.set(JsonFormatOptions.IGNORE_PARSE_ERRORS, true);
    var schema = createDecoder(options, DataTypes.INT());

    var rows = deserialize(schema, "{\"value\":\"not-an-integer\"}");

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).isNullAt(0)).isTrue();
  }

  @Test
  void givenRawJsonField_whenDeserialize_thenWrapsPayloadInFlinkJsonType() throws Exception {
    var payloadType = new RawType<>(FlinkJsonType.class, new FlinkJsonTypeSerializer());
    var rowType = RowType.of(new LogicalType[] {payloadType}, new String[] {"payload"});
    var mapper = new ObjectMapper();
    var schema =
        new SqrlJsonRowDataDeserializationSchema(
            rowType, TypeInformation.of(RowData.class), false, false, TimestampFormat.ISO_8601);

    var row = schema.convertToRowData(mapper.readTree("{\"payload\":{\"id\":42}}"));
    RawValueData<FlinkJsonType> payload = row.getRawValue(0);

    assertThat(payload.toObject(payloadType.getTypeSerializer()).getJson())
        .isEqualTo(mapper.readTree("{\"id\":42}"));
  }

  private SqrlJsonRowDataDeserializationSchema createDecoder(
      Configuration options, DataType fieldType) throws Exception {
    var decodingFormat =
        (ProjectableDecodingFormat<DeserializationSchema<RowData>>)
            new FlexibleJsonFormat().createDecodingFormat(null, options);
    var context = mock(DynamicTableSource.Context.class);
    when(context.<RowData>createTypeInformation(any(DataType.class)))
        .thenReturn(TypeInformation.of(RowData.class));
    var rowType = DataTypes.ROW(DataTypes.FIELD("value", fieldType));
    var schema =
        (SqrlJsonRowDataDeserializationSchema)
            decodingFormat.createRuntimeDecoder(context, rowType, new int[][] {{0}});
    schema.open(null);
    return schema;
  }

  private List<RowData> deserialize(SqrlJsonRowDataDeserializationSchema schema, String json)
      throws Exception {
    var rows = new ArrayList<RowData>();
    schema.deserialize(
        json.getBytes(StandardCharsets.UTF_8),
        new org.apache.flink.util.Collector<>() {
          @Override
          public void collect(RowData row) {
            rows.add(row);
          }

          @Override
          public void close() {}
        });
    return rows;
  }
}
