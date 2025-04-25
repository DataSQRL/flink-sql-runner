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
package com.datasqrl.flinkrunner.format.json;

import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions.MapNullKeyMode;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;

/**
 * Extends the {@link RowDataToJsonConverters} to add support for FlinkJsonType by serializing it as
 * json and not string
 */
public class SqrlRowDataToJsonConverters extends RowDataToJsonConverters {

  public SqrlRowDataToJsonConverters(
      TimestampFormat timestampFormat, MapNullKeyMode mapNullKeyMode, String mapNullKeyLiteral) {
    super(timestampFormat, mapNullKeyMode, mapNullKeyLiteral);
  }

  @Override
  public RowDataToJsonConverter createConverter(LogicalType type) {

    switch (type.getTypeRoot()) {
      case RAW:
        // sqrl add raw type
        var rawType = (RawType) type;
        if (rawType.getOriginatingClass() == FlinkJsonType.class) {
          return createJsonConverter((RawType) type);
        }
    }
    return super.createConverter(type);
  }

  private RowDataToJsonConverter createJsonConverter(RawType type) {
    return (mapper, reuse, value) -> {
      if (value == null) {
        return null;
      }
      var binaryRawValueData = (BinaryRawValueData) value;
      var o = (FlinkJsonType) binaryRawValueData.toObject(type.getTypeSerializer());
      if (o == null) {
        return null;
      }
      return o.getJson();
    };
  }
}
