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

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class SqrlJsonRowDataDeserializationSchemaTest {

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
}
