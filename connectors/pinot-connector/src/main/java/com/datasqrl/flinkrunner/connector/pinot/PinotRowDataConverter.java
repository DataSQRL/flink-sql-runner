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

import java.io.Serializable;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.pinot.spi.data.readers.GenericRow;

/**
 * Converts a Flink {@link RowData} (internal binary format) into a Pinot {@link GenericRow}.
 *
 * <p>Field getters are transient because {@link RowData.FieldGetter} is not serializable. They are
 * rebuilt lazily on first use after deserialization.
 *
 * <p>Type mapping (Flink internal → Pinot / Java):
 *
 * <pre>
 *  BOOLEAN                        → Boolean
 *  TINYINT / SMALLINT / INTEGER   → Integer
 *  BIGINT                         → Long
 *  FLOAT                          → Float
 *  DOUBLE                         → Double
 *  DECIMAL                        → Double  (precision may be lost for very large decimals)
 *  CHAR / VARCHAR                 → String
 *  DATE                           → Integer (days since epoch)
 *  TIME_WITHOUT_TIME_ZONE         → Integer (millis since midnight)
 *  TIMESTAMP / TIMESTAMP_LTZ      → Long    (epoch millis)
 *  BINARY / VARBINARY             → byte[]
 * </pre>
 */
@RequiredArgsConstructor
public class PinotRowDataConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String[] fieldNames;
  private final DataType[] fieldDataTypes;

  private transient RowData.FieldGetter[] fieldGetters;
  private transient LogicalType[] logicalTypes;

  private void ensureInitialized() {
    if (fieldGetters != null) {
      return;
    }
    logicalTypes = new LogicalType[fieldDataTypes.length];
    fieldGetters = new RowData.FieldGetter[fieldDataTypes.length];
    for (int i = 0; i < fieldDataTypes.length; i++) {
      logicalTypes[i] = fieldDataTypes[i].getLogicalType();
      fieldGetters[i] = RowData.createFieldGetter(logicalTypes[i], i);
    }
  }

  public GenericRow convert(RowData element) {
    ensureInitialized();
    var row = new GenericRow();
    for (int i = 0; i < fieldNames.length; i++) {
      row.putValue(
          fieldNames[i], toExternal(fieldGetters[i].getFieldOrNull(element), logicalTypes[i]));
    }
    return row;
  }

  private Object toExternal(Object internal, LogicalType type) {
    if (internal == null) {
      return null;
    }
    return switch (type.getTypeRoot()) {
      case CHAR, VARCHAR -> internal.toString();
      case BOOLEAN -> internal;
      case TINYINT -> ((Number) internal).byteValue();
      case SMALLINT -> ((Number) internal).shortValue();
      case INTEGER, DATE, TIME_WITHOUT_TIME_ZONE -> ((Number) internal).intValue();
      case BIGINT -> ((Number) internal).longValue();
      case FLOAT -> ((Number) internal).floatValue();
      case DOUBLE -> ((Number) internal).doubleValue();
      case DECIMAL -> ((DecimalData) internal).toBigDecimal().doubleValue();
      case TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          ((TimestampData) internal).getMillisecond();
      case BINARY, VARBINARY -> (byte[]) internal;
      default ->
          throw new UnsupportedOperationException(
              "No Pinot type mapping for Flink type: " + type.getTypeRoot());
    };
  }
}
