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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;

@Slf4j
@AutoService(TableFunction.class)
public class read_partition_sizes extends TableFunction<Row> {

  /**
   * Computes the total size on disk for each partition in an Iceberg table.
   *
   * @param warehouse The warehouse path or URI for the catalog
   * @param catalogType The type of catalog (optional, e.g., "hadoop", "hive")
   * @param catalogName The name of the catalog
   * @param databaseName The database/schema name (optional)
   * @param tableName The name of the table to analyze
   */
  @FunctionHint(
      output = @DataTypeHint("ROW<partition_map MAP<STRING, STRING>, partition_size BIGINT>"))
  public void eval(
      String warehouse,
      @Nullable String catalogType,
      String catalogName,
      @Nullable String databaseName,
      String tableName) {

    if (warehouse == null || catalogName == null || tableName == null) {
      return;
    }

    Function<Table, Void> readFn =
        table -> {
          var partitions = new HashMap<Map<String, String>, Long>();

          try (var tasks = table.newScan().planFiles()) {
            for (var task : tasks) {

              var partitionData = extractPartitionData(task);
              var partitionMap = partitionData.f0;
              var fileSize = partitionData.f1;

              partitions.merge(partitionMap, fileSize, Long::sum);
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to compute partition sizes", e);
          }

          partitions.entrySet().stream()
              .map(e -> Row.of(e.getKey(), e.getValue()))
              .forEach(this::collect);

          return null;
        };

    CatalogUtils.executeInCatalog(
        warehouse, catalogType, catalogName, databaseName, tableName, readFn);
  }

  /**
   * Extracts partition data and file size from a file scan task.
   *
   * @param task the Iceberg file scan task
   * @return tuple of (partition column map, file_size) where map contains column name -> string
   *     value
   */
  private Tuple2<Map<String, String>, Long> extractPartitionData(FileScanTask task) {
    var dataFile = task.file();
    var spec = task.spec();
    var size = dataFile.fileSizeInBytes();
    var partitionMap = new HashMap<String, String>();

    if (spec.isUnpartitioned()) {
      return Tuple2.of(partitionMap, size);
    }

    var p = dataFile.partition();
    for (int i = 0; i < spec.fields().size(); i++) {
      var field = spec.fields().get(i);
      var value = p.get(i, Object.class);
      partitionMap.put(field.name(), value != null ? value.toString() : null);
    }

    return Tuple2.of(partitionMap, size);
  }
}
