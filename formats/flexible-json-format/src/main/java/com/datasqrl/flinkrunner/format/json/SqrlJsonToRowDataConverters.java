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

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;

/** Adds JSON deserialization support for the SQRL JSON RAW type. */
public class SqrlJsonToRowDataConverters extends JsonToRowDataConverters {

  public SqrlJsonToRowDataConverters(
      boolean failOnMissingField, boolean ignoreParseErrors, TimestampFormat timestampFormat) {
    super(failOnMissingField, ignoreParseErrors, timestampFormat);
  }

  @Override
  public JsonToRowDataConverter createConverter(LogicalType type) {
    if (type.getTypeRoot() == LogicalTypeRoot.RAW) {
      var rawType = (RawType<?>) type;
      if (rawType.getOriginatingClass() == FlinkJsonType.class) {
        return jsonNode -> {
          if (jsonNode == null || jsonNode.isNull() || jsonNode.isMissingNode()) {
            return null;
          }
          return RawValueData.fromObject(new FlinkJsonType(jsonNode));
        };
      }
    }
    return super.createConverter(type);
  }
}
