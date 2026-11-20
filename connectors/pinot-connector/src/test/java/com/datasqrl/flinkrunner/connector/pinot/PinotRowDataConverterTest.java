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
package com.datasqrl.flinkrunner.connector.pinot;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class PinotRowDataConverterTest {

  @Test
  void convertsPrimitiveTypes() {
    var fieldNames = new String[] {"b", "i", "l", "f", "d", "s"};
    var fieldTypes =
        new DataType[] {
          DataTypes.BOOLEAN(),
          DataTypes.INT(),
          DataTypes.BIGINT(),
          DataTypes.FLOAT(),
          DataTypes.DOUBLE(),
          DataTypes.STRING()
        };

    var row =
        GenericRowData.ofKind(
            RowKind.INSERT, true, 42, 100L, 1.5f, 3.14, StringData.fromString("hello"));
    var converter = new PinotRowDataConverter(fieldNames, fieldTypes);
    var result = converter.convert(row);

    assertThat(result.getValue("b")).isEqualTo(true);
    assertThat(result.getValue("i")).isEqualTo(42);
    assertThat(result.getValue("l")).isEqualTo(100L);
    assertThat(result.getValue("f")).isEqualTo(1.5f);
    assertThat(result.getValue("d")).isEqualTo(3.14);
    assertThat(result.getValue("s")).isEqualTo("hello");
  }

  @Test
  void convertsTimestampToEpochMillis() {
    var fieldNames = new String[] {"ts"};
    var fieldTypes = new DataType[] {DataTypes.TIMESTAMP_LTZ(3)};

    var row =
        GenericRowData.ofKind(RowKind.INSERT, TimestampData.fromEpochMillis(1_700_000_000_000L));
    var converter = new PinotRowDataConverter(fieldNames, fieldTypes);
    var result = converter.convert(row);

    assertThat(result.getValue("ts")).isEqualTo(1_700_000_000_000L);
  }

  @Test
  void convertsDecimalToDouble() {
    var fieldNames = new String[] {"amount"};
    var fieldTypes = new DataType[] {DataTypes.DECIMAL(10, 2)};

    var row =
        GenericRowData.ofKind(
            RowKind.INSERT, DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
    var converter = new PinotRowDataConverter(fieldNames, fieldTypes);
    var result = converter.convert(row);

    assertThat((Double) result.getValue("amount")).isEqualTo(123.45);
  }

  @Test
  void propagatesNullValues() {
    var fieldNames = new String[] {"id", "name"};
    var fieldTypes = new DataType[] {DataTypes.BIGINT(), DataTypes.STRING()};

    var row = GenericRowData.ofKind(RowKind.INSERT, 1L, null);
    var converter = new PinotRowDataConverter(fieldNames, fieldTypes);
    var result = converter.convert(row);

    assertThat(result.getValue("id")).isEqualTo(1L);
    assertThat(result.getValue("name")).isNull();
  }

  @Test
  void reinitializesAfterDeserialization() throws Exception {
    var fieldNames = new String[] {"val"};
    var fieldTypes = new DataType[] {DataTypes.INT()};
    var converter = new PinotRowDataConverter(fieldNames, fieldTypes);

    // Simulate deserialization by cloning through Java serialization
    var baos = new java.io.ByteArrayOutputStream();
    new java.io.ObjectOutputStream(baos).writeObject(converter);
    var deserialized =
        (PinotRowDataConverter)
            new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(baos.toByteArray()))
                .readObject();

    var row = GenericRowData.ofKind(RowKind.INSERT, 99);
    var result = deserialized.convert(row);

    assertThat(result.getValue("val")).isEqualTo(99);
  }
}
