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
package com.datasqrl.flinkrunner.stdlib.iceberg;

import com.google.auto.service.AutoService;
import javax.annotation.Nullable;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;

@AutoService(TableFunction.class)
public class read_partition_sizes_str extends AbstractPartitionSizeReader<String> {

  @FunctionHint(
      output = @DataTypeHint("ROW<partition_id STRING, time_bucket BIGINT, partition_size BIGINT>"))
  public void eval(
      String warehouse,
      @Nullable String catalogType,
      String catalogName,
      @Nullable String databaseName,
      String tableName,
      String partitionCol) {

    readPartitionSizes(warehouse, catalogType, catalogName, databaseName, tableName, partitionCol);
  }

  @Override
  String getUnpartitionedId() {
    return "__UNPARTITIONED__";
  }
}
