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
package com.datasqrl.flinkrunner.connector.postgresql.jdbc;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SqrlPostgresOptions {

  public enum OnConflictAction {
    UPDATE,
    TIMESTAMP,
    IGNORE
  }

  public static final ConfigOption<OnConflictAction> SINK_ON_CONFLICT =
      ConfigOptions.key("sink.on-conflict.action")
          .enumType(OnConflictAction.class)
          .defaultValue(OnConflictAction.UPDATE);

  public static final ConfigOption<String> SINK_ON_CONFLICT_COLUMN =
      ConfigOptions.key("sink.on-conflict.timestamp-column").stringType().noDefaultValue();

  public static void validateOnConflictOptions(ReadableConfig tableOptions, DataType dataType) {
    var onConflict = tableOptions.get(SINK_ON_CONFLICT);
    var timestampColumn = tableOptions.get(SINK_ON_CONFLICT_COLUMN);

    if (onConflict == OnConflictAction.TIMESTAMP && timestampColumn == null) {
      throw new IllegalArgumentException(
          "'%s' is set to '%s', but '%s' is not specified."
              .formatted(
                  SINK_ON_CONFLICT.key(),
                  OnConflictAction.TIMESTAMP,
                  SINK_ON_CONFLICT_COLUMN.key()));
    }

    if (onConflict == OnConflictAction.TIMESTAMP) {
      var fieldNames = DataType.getFieldNames(dataType);
      var fieldIndex = fieldNames.indexOf(timestampColumn);

      if (fieldIndex < 0) {
        throw new IllegalArgumentException(
            "'%s' is set to '%s', but no such column exists in the table schema."
                .formatted(SINK_ON_CONFLICT_COLUMN.key(), timestampColumn));
      }

      var fieldType = DataType.getFieldDataTypes(dataType).get(fieldIndex).getLogicalType();
      if (!fieldType.isAnyOf(LogicalTypeFamily.TIMESTAMP)) {
        throw new IllegalArgumentException(
            "'%s' is set to '%s', but the column type '%s' is not a TIMESTAMP type."
                .formatted(SINK_ON_CONFLICT_COLUMN.key(), timestampColumn, fieldType));
      }
    }
  }
}
