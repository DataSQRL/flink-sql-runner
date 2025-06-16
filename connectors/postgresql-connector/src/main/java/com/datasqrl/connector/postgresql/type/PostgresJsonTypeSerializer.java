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
package com.datasqrl.connector.postgresql.type;

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import com.google.auto.service.AutoService;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcDeserializationConverter;
import org.apache.flink.connector.jdbc.converter.AbstractJdbcRowConverter.JdbcSerializationConverter;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.postgresql.util.PGobject;

@AutoService(JdbcTypeSerializer.class)
public class PostgresJsonTypeSerializer
    implements JdbcTypeSerializer<JdbcDeserializationConverter, JdbcSerializationConverter> {

  @Override
  public String getDialectId() {
    return "postgres";
  }

  @Override
  public Class getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public String dialectTypeName() {
    return "jsonb";
  }

  @Override
  public GenericDeserializationConverter<JdbcDeserializationConverter> getDeserializerConverter() {
    return () ->
        (val) -> {
          FlinkJsonType t = (FlinkJsonType) val;
          return t.getJson();
        };
  }

  @Override
  public GenericSerializationConverter<JdbcSerializationConverter> getSerializerConverter(
      LogicalType type) {
    FlinkJsonTypeSerializer typeSerializer = new FlinkJsonTypeSerializer();

    return () ->
        (val, index, statement) -> {
          if (val != null && !val.isNullAt(index)) {
            PGobject pgObject = new PGobject();
            pgObject.setType("json");
            RawValueData<FlinkJsonType> object = val.getRawValue(index);
            FlinkJsonType vec = object.toObject(typeSerializer);
            if (vec == null) {
              statement.setObject(index, null);
            } else {
              pgObject.setValue(vec.getJson().toString());
              statement.setObject(index, pgObject);
            }
          } else {
            statement.setObject(index, null);
          }
        };
  }
}
