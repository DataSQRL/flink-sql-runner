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

import static com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.SINK_ON_CONFLICT;
import static com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.SINK_ON_CONFLICT_COLUMN;
import static com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.validateOnConflictOptions;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.OnConflictAction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

class SqrlPostgresOptionsTest {

  private static final DataType ROW_TYPE =
      DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.BIGINT()),
          DataTypes.FIELD("value", DataTypes.STRING()),
          DataTypes.FIELD("event_time", DataTypes.TIMESTAMP(3)));

  @Test
  void defaultActionWithoutTimestampColumnPasses() {
    var config = new Configuration();

    assertThatCode(() -> validateOnConflictOptions(config, ROW_TYPE)).doesNotThrowAnyException();
  }

  @Test
  void timestampActionWithValidTimestampColumnPasses() {
    var config = new Configuration();
    config.set(SINK_ON_CONFLICT, OnConflictAction.TIMESTAMP);
    config.set(SINK_ON_CONFLICT_COLUMN, "event_time");

    assertThatCode(() -> validateOnConflictOptions(config, ROW_TYPE)).doesNotThrowAnyException();
  }

  @Test
  void timestampActionWithoutTimestampColumnThrows() {
    var config = new Configuration();
    config.set(SINK_ON_CONFLICT, OnConflictAction.TIMESTAMP);

    assertThatThrownBy(() -> validateOnConflictOptions(config, ROW_TYPE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(SINK_ON_CONFLICT.key())
        .hasMessageContaining(OnConflictAction.TIMESTAMP.name())
        .hasMessageContaining(SINK_ON_CONFLICT_COLUMN.key());
  }

  @Test
  void unknownTimestampColumnThrows() {
    var config = new Configuration();
    config.set(SINK_ON_CONFLICT, OnConflictAction.TIMESTAMP);
    config.set(SINK_ON_CONFLICT_COLUMN, "missing_column");

    assertThatThrownBy(() -> validateOnConflictOptions(config, ROW_TYPE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing_column")
        .hasMessageContaining("no such column");
  }

  @Test
  void nonTimestampColumnTypeThrows() {
    var config = new Configuration();
    config.set(SINK_ON_CONFLICT, OnConflictAction.TIMESTAMP);
    config.set(SINK_ON_CONFLICT_COLUMN, "value");

    assertThatThrownBy(() -> validateOnConflictOptions(config, ROW_TYPE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("value")
        .hasMessageContaining("not a TIMESTAMP type");
  }

  @Test
  void timestampLtzColumnIsAccepted() {
    var rowType =
        DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3)));
    var config = new Configuration();
    config.set(SINK_ON_CONFLICT, OnConflictAction.TIMESTAMP);
    config.set(SINK_ON_CONFLICT_COLUMN, "event_time");

    assertThatCode(() -> validateOnConflictOptions(config, rowType)).doesNotThrowAnyException();
  }
}
