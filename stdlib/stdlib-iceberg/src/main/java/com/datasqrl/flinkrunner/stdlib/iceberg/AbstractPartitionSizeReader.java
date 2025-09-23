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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

abstract class AbstractPartitionSizeReader<T> extends TableFunction<Row> {

  private transient Map<String, Integer> posByName = null;

  abstract T getUnpartitionedId();

  void readPartitionSizes(
      String warehouse,
      @Nullable String catalogType,
      String catalogName,
      @Nullable String databaseName,
      String tableName,
      String partitionCol) {

    if (warehouse == null || catalogName == null || tableName == null || partitionCol == null) {
      return;
    }

    Function<Table, Void> readFn =
        table -> {
          var partitions = new HashMap<String, Tuple3<T, Long, Long>>();

          try (var tasks = table.newScan().planFiles()) {
            for (var task : tasks) {

              var partitionData = extractPartitionData(task, partitionCol);
              // concat partition_id + time_bucket as key
              var key = partitionData.f0 + "_" + partitionData.f1;

              partitions.merge(
                  key, partitionData, (t1, t2) -> Tuple3.of(t1.f0, t1.f1, t1.f2 + t2.f2));
            }
          } catch (Exception e) {
            throw new RuntimeException("Failed to compute partition sizes", e);
          }

          for (var values : partitions.values()) {
            collect(Row.of(values.f0, values.f1, values.f2));
          }

          return null;
        };

    CatalogUtils.executeInCatalog(
        warehouse, catalogType, catalogName, databaseName, tableName, readFn);
  }

  /**
   * Extracts partition ID, time bucket, and file size from a file scan task.
   *
   * @param task the Iceberg file scan task
   * @param partitionCol the name of the partition colum in the table
   * @return tuple of (partition_id, time_bucket, file_size)
   */
  private Tuple3<T, Long, Long> extractPartitionData(FileScanTask task, String partitionCol) {
    var dataFile = task.file();
    var spec = task.spec();
    var size = dataFile.fileSizeInBytes();

    if (spec.isUnpartitioned()) {
      return Tuple3.of(getUnpartitionedId(), null, size);
    }

    if (posByName == null) {
      posByName = calculateFieldPosByName(spec, partitionCol, "time_bucket");
    }

    var p = dataFile.partition();
    var partitionIdVal = (T) p.get(posByName.get(partitionCol), Object.class);
    var timeBucketVal = p.get(posByName.get("time_bucket"), Long.class);

    return Tuple3.of(partitionIdVal, timeBucketVal, size);
  }

  private Map<String, Integer> calculateFieldPosByName(PartitionSpec spec, String... fieldNames) {
    var m = new HashMap<String, Integer>(fieldNames.length);
    var names = Set.of(fieldNames);

    for (int i = 0; i < spec.fields().size() || m.size() < fieldNames.length; ++i) {
      var name = spec.fields().get(i).name();
      if (names.contains(name)) {
        m.put(name, i);
      }
    }

    return m;
  }
}
