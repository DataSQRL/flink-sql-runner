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

import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.iceberg.Table;

abstract class DeduplicatedDataDeleter extends ScalarFunction {

  boolean deleteDeduplicatedData(
      String warehouse,
      @Nullable String catalogType,
      String catalogName,
      @Nullable String databaseName,
      String tableName,
      String partitionCol,
      Long maxTimeBucket,
      Object[] partitionIds) {

    if (warehouse == null
        || catalogName == null
        || tableName == null
        || partitionCol == null
        || maxTimeBucket == null
        || partitionIds == null
        || partitionIds.length < 1) {
      return false;
    }

    var delFn = getDeleteFunction(partitionCol, partitionIds, maxTimeBucket);
    CatalogUtils.executeInCatalog(
        warehouse, catalogType, catalogName, databaseName, tableName, delFn);

    return true;
  }

  Object[] toArray(Map<?, ?> partitionIds) {
    return partitionIds == null ? null : partitionIds.keySet().toArray();
  }

  private Function<Table, Void> getDeleteFunction(
      String partitionIdCol, Object[] partitionIdVals, Long maxTimeBucket) {

    return table -> {
      var delExpr =
          ExpressionUtils.buildPartitionDelete(partitionIdCol, partitionIdVals, maxTimeBucket);
      var delFiles = table.newDelete().deleteFromRowFilter(delExpr);
      delFiles.commit();

      return null;
    };
  }
}
