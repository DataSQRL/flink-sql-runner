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

import java.io.IOException;
import java.io.Serial;
import lombok.NonNull;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.AbstractJsonDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

/** Deserializes JSON rows with support for SQRL-specific types. */
public class SqrlJsonRowDataDeserializationSchema extends AbstractJsonDeserializationSchema {

  @Serial private static final long serialVersionUID = 1L;

  private final SqrlJsonToRowDataConverters.JsonToRowDataConverter runtimeConverter;

  public SqrlJsonRowDataDeserializationSchema(
      @NonNull RowType rowType,
      TypeInformation<RowData> resultTypeInfo,
      boolean failOnMissingField,
      boolean ignoreParseErrors,
      TimestampFormat timestampFormat) {
    super(rowType, resultTypeInfo, failOnMissingField, ignoreParseErrors, timestampFormat);
    runtimeConverter =
        new SqrlJsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
            .createConverter(rowType);
  }

  // Borrowed as is from org.apache.flink.formats.json.JsonRowDataDeserializationSchema
  @Override
  public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
    if (message == null) {
      return;
    }
    try {
      var root = deserializeToJsonNode(message);
      if (root != null && root.isArray()) {
        var arrayNode = (ArrayNode) root;
        for (int i = 0; i < arrayNode.size(); i++) {
          try {
            var result = convertToRowData(arrayNode.get(i));
            if (result != null) {
              out.collect(result);
            }
          } catch (Throwable t) {
            if (!ignoreParseErrors) {
              throw t;
            }
          }
        }
      } else {
        var result = convertToRowData(root);
        if (result != null) {
          out.collect(result);
        }
      }
    } catch (Throwable t) {
      if (!ignoreParseErrors) {
        throw new IOException("Failed to deserialize JSON '%s'.".formatted(new String(message)), t);
      }
    }
  }

  public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
    return objectMapper.readTree(message);
  }

  public RowData convertToRowData(JsonNode message) {
    return (RowData) runtimeConverter.convert(message);
  }
}
