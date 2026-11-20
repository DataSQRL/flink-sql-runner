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

import java.util.Collections;
import java.util.HashMap;
import lombok.RequiredArgsConstructor;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

/** Planner-facing description of the Pinot OFFLINE sink. */
@RequiredArgsConstructor
public class PinotDynamicTableSink implements DynamicTableSink {

  private static final String DEFAULT_OUTPUT_DIR = "/tmp/pinotoutput";

  private final String controllerUrl;
  private final String tableName;
  private final long segmentFlushRows;
  private final DataType physicalRowDataType;

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.insertOnly();
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    var fieldNames = DataType.getFieldNames(physicalRowDataType).toArray(new String[0]);
    var fieldDataTypes = DataType.getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]);

    var converter = new PinotRowDataConverter(fieldNames, fieldDataTypes);
    var schema = buildPinotSchema(fieldNames, fieldDataTypes);
    var tableConfig = buildTableConfig();

    return SinkV2Provider.of(
        new PinotRowDataSink(tableConfig, schema, segmentFlushRows, converter));
  }

  @Override
  public DynamicTableSink copy() {
    return new PinotDynamicTableSink(
        controllerUrl, tableName, segmentFlushRows, physicalRowDataType);
  }

  @Override
  public String asSummaryString() {
    return "Pinot[" + tableName + "]";
  }

  private Schema buildPinotSchema(String[] fieldNames, DataType[] fieldDataTypes) {
    var builder = new Schema.SchemaBuilder().setSchemaName(tableName);
    for (int i = 0; i < fieldNames.length; i++) {
      builder.addSingleValueDimension(
          fieldNames[i], toPinotDataType(fieldDataTypes[i].getLogicalType()));
    }
    return builder.build();
  }

  private TableConfig buildTableConfig() {
    var batchConfigMap = new HashMap<String, String>();
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, DEFAULT_OUTPUT_DIR);
    batchConfigMap.put(BatchConfigProperties.PUSH_CONTROLLER_URI, controllerUrl);

    var ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", "HOURLY"));

    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setIngestionConfig(ingestionConfig)
        .build();
  }

  private FieldSpec.DataType toPinotDataType(LogicalType type) {
    return switch (type.getTypeRoot()) {
      case BOOLEAN -> FieldSpec.DataType.BOOLEAN;
      case TINYINT, SMALLINT, INTEGER, DATE, TIME_WITHOUT_TIME_ZONE -> FieldSpec.DataType.INT;
      case BIGINT, TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE ->
          FieldSpec.DataType.LONG;
      case FLOAT -> FieldSpec.DataType.FLOAT;
      case DOUBLE, DECIMAL -> FieldSpec.DataType.DOUBLE;
      case CHAR, VARCHAR -> FieldSpec.DataType.STRING;
      case BINARY, VARBINARY -> FieldSpec.DataType.BYTES;
      default ->
          throw new UnsupportedOperationException(
              "No Pinot type mapping for Flink type: " + type.getTypeRoot());
    };
  }
}
