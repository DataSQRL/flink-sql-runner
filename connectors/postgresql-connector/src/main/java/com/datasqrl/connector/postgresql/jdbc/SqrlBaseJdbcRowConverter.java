/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.connector.postgresql.jdbc;

import static com.datasqrl.connector.postgresql.type.FlinkArrayTypeUtil.getBaseFlinkArrayType;
import static com.datasqrl.connector.postgresql.type.FlinkArrayTypeUtil.isScalarArray;
import static com.datasqrl.connector.postgresql.type.PostgresArrayTypeConverter.getArrayScalarName;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/** A sqrl class to handle arrays and extra data types */
public abstract class SqrlBaseJdbcRowConverter extends AbstractJdbcRowConverter {

  public SqrlBaseJdbcRowConverter(RowType rowType) {
    super(rowType);
  }

  @Override
  protected JdbcSerializationConverter wrapIntoNullableExternalConverter(
      JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
    if (type.getTypeRoot() == TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      int timestampWithTimezone = Types.TIMESTAMP_WITH_TIMEZONE;
      return (val, index, statement) -> {
        if (val == null || val.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
          statement.setNull(index, timestampWithTimezone);
        } else {
          jdbcSerializationConverter.serialize(val, index, statement);
        }
      };
    }
    return super.wrapIntoNullableExternalConverter(jdbcSerializationConverter, type);
  }

  @Override
  public JdbcDeserializationConverter createInternalConverter(LogicalType type) {
    LogicalTypeRoot root = type.getTypeRoot();

    if (root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return val ->
          val instanceof LocalDateTime ldt
              ? TimestampData.fromLocalDateTime(ldt)
              : TimestampData.fromTimestamp((Timestamp) val);
    } else if (root == LogicalTypeRoot.ARRAY) {
      ArrayType arrayType = (ArrayType) type;
      return createArrayConverter(arrayType);
    } else if (root == LogicalTypeRoot.ROW) {
      return val -> val;
    } else if (root == LogicalTypeRoot.MAP) {
      return val -> val;
    } else {
      return super.createInternalConverter(type);
    }
  }

  @Override
  protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
    switch (type.getTypeRoot()) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int tsPrecision = ((LocalZonedTimestampType) type).getPrecision();
        return (val, index, statement) ->
            statement.setTimestamp(index, val.getTimestamp(index, tsPrecision).toTimestamp());
      case MULTISET:
      case RAW:
      default:
        return super.createExternalConverter(type);
    }
  }

  @SneakyThrows
  private void createSqlArrayObject(
      LogicalType type, ArrayData data, int idx, PreparedStatement statement) {
    // Scalar arrays of any dimension are one array call
    if (isScalarArray(type)) {
      Object[] boxed;
      if (data instanceof GenericArrayData arrayData) {
        boxed = arrayData.toObjectArray();
      } else if (data instanceof BinaryArrayData arrayData) {
        boxed = arrayData.toObjectArray(getBaseFlinkArrayType(type));
      } else {
        throw new RuntimeException("Unsupported ArrayData type: " + data.getClass());
      }
      Array array = statement.getConnection().createArrayOf(getArrayScalarName(type), boxed);
      statement.setArray(idx, array);
    } else {
      // If it is not a scalar array (e.g. row type), use an empty byte array.
      Array array = statement.getConnection().createArrayOf(getArrayType(), new Byte[0]);
      statement.setArray(idx, array);
    }
  }

  protected abstract String getArrayType();

  public abstract JdbcDeserializationConverter createArrayConverter(ArrayType arrayType);
}
