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
package com.datasqrl.connector.postgresql.type;

import com.datasqrl.types.vector.FlinkVectorType;
import com.datasqrl.types.vector.FlinkVectorTypeSerializer;
import java.util.Arrays;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

public class PostgresVectorTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialectId() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return FlinkVectorType.class;
  }

  @Override
  public String dialectTypeName() {
    return "vector";
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () ->
        (val) -> {
          var t = (FlinkVectorType) val;
          return t.getValue();
        };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    var flinkVectorTypeSerializer = new FlinkVectorTypeSerializer();
    return () ->
        (val, index, statement) -> {
          if (val != null && !val.isNullAt(index)) {
            RawValueData<FlinkVectorType> object = val.getRawValue(index);
            var vec = object.toObject(flinkVectorTypeSerializer);

            if (vec != null) {
              var pgObject = new PGobject();
              pgObject.setType("vector");
              pgObject.setValue(Arrays.toString(vec.getValue()));
              statement.setObject(index, pgObject);
              return;
            }
          }
          statement.setObject(index, null);
        };
  }
}
