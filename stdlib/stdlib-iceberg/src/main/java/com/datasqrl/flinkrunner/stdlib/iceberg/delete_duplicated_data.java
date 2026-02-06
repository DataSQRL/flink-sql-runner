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
package com.datasqrl.flinkrunner.stdlib.iceberg;

import com.google.auto.service.AutoService;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Type;

@Slf4j
@AutoService(ScalarFunction.class)
public class delete_duplicated_data extends ScalarFunction {

  private static final String DEFAULT_TIME_BUCKET = "time_bucket";

  /**
   * Deletes duplicated data from an Iceberg table based on partition specifications and time bucket
   * constraints.
   *
   * @param warehouse The warehouse path or URI for the catalog
   * @param catalogType The type of catalog (optional, e.g., "hadoop", "hive")
   * @param catalogName The name of the catalog
   * @param databaseName The database/schema name (optional)
   * @param tableName The name of the table to delete from
   * @param maxTimeBucket The maximum time bucket value (inclusive) for records to delete
   * @param partitionSet Set of partition specifications where each map contains column name ->
   *     value mappings. Maps can be empty for tables partitioned only by time bucket column. All
   *     maps must have the same keyset (same partition columns).
   * @return true if deletion was successful, false if invalid parameters provided
   */
  public boolean eval(
      String warehouse,
      @Nullable String catalogType,
      String catalogName,
      @Nullable String databaseName,
      String tableName,
      Long maxTimeBucket,
      @DataTypeHint("MULTISET<MAP<STRING, STRING>>")
          Map<Map<String, String>, Integer> partitionSet) {

    if (warehouse == null
        || catalogName == null
        || tableName == null
        || maxTimeBucket == null
        || partitionSet == null
        || partitionSet.isEmpty()) {
      log.debug("Invalid input, nothing to do");
      return false;
    }

    var partitionColsOpt = Optional.<Set<String>>empty();
    for (var partitionMap : partitionSet.keySet()) {
      if (partitionColsOpt.isEmpty()) {
        partitionColsOpt = Optional.of(new HashSet<>(partitionMap.keySet()));
      } else if (!partitionColsOpt.get().equals(partitionMap.keySet())) {
        // Return false instead of throwing exception to avoid crashing Flink job
        log.debug("Inconsistent partition column set: {}", partitionSet);
        return false;
      }
    }

    // Make final copy for lambda expression
    final var partitionCols = partitionColsOpt.get();

    try {
      Function<Table, Boolean> delFn =
          table -> {
            var spec = table.spec();
            var timeBucketCol = findTimeBucketColumn(spec, partitionCols);

            // If no time bucket column found, cannot proceed
            if (timeBucketCol == null) {
              log.debug("Time bucket column not found");
              return false;
            }

            // Build delete expression
            var delExpr =
                buildGeneralizedPartitionDelete(
                    spec, partitionSet.keySet(), timeBucketCol, maxTimeBucket);

            // Execute delete
            var delFiles = table.newDelete().deleteFromRowFilter(delExpr);
            delFiles.commit();

            return true;
          };

      return CatalogUtils.executeInCatalog(
          warehouse, catalogType, catalogName, databaseName, tableName, delFn);

    } catch (Exception e) {
      // Return false instead of letting exception propagate
      return false;
    }
  }

  private String findTimeBucketColumn(PartitionSpec spec, Set<String> partitionColumns) {
    // Handle unpartitioned tables
    if (spec.isUnpartitioned()) {
      log.debug("Table in unpartitioned");
      return null;
    }

    var allPartitionFields = new HashSet<String>();
    for (var field : spec.fields()) {
      allPartitionFields.add(field.name());
    }

    // Find the time bucket column by removing known partition columns
    var remainingFields = new HashSet<>(allPartitionFields);
    remainingFields.removeAll(partitionColumns);

    if (remainingFields.size() == 1) {
      return remainingFields.iterator().next();

    } else if (remainingFields.contains(DEFAULT_TIME_BUCKET)) {
      return DEFAULT_TIME_BUCKET;
    }

    log.debug(
        "Cannot determine time bucket column from the given partition column set: {}",
        allPartitionFields);
    return null;
  }

  private Expression buildGeneralizedPartitionDelete(
      PartitionSpec spec,
      Set<Map<String, String>> partitionMaps,
      String timeBucketCol,
      Long maxTimeBucket) {

    // Build time bucket conditions
    var timeBucketExpr =
        Expressions.and(
            Expressions.greaterThan(timeBucketCol, 0),
            Expressions.lessThanOrEqual(timeBucketCol, maxTimeBucket));

    // If partition maps are empty (only time bucket column), return just time conditions
    if (partitionMaps.isEmpty()
        || (partitionMaps.size() == 1 && partitionMaps.iterator().next().isEmpty())) {
      return timeBucketExpr;
    }

    // Build OR expression for all partition maps
    Expression partitionExpr = null;
    for (var partitionMap : partitionMaps) {
      if (partitionMap.isEmpty()) {
        // Skip empty partition maps (they would match everything)
        continue;
      }

      Expression andExpr = null;

      // Build AND expression for all columns in this partition map
      for (var entry : partitionMap.entrySet()) {
        var columnName = entry.getKey();
        var stringValue = entry.getValue();

        // Cast value to appropriate type based on partition field type
        var typedValue = castValue(spec, columnName, stringValue);
        var colExpr = Expressions.equal(columnName, typedValue);

        andExpr = andExpr == null ? colExpr : Expressions.and(andExpr, colExpr);
      }

      if (andExpr != null) {
        if (partitionExpr == null) {
          partitionExpr = andExpr;
        } else {
          partitionExpr = Expressions.or(partitionExpr, andExpr);
        }
      }
    }

    // Combine with time bucket conditions
    if (partitionExpr == null) {
      return timeBucketExpr;
    }

    return Expressions.and(timeBucketExpr, partitionExpr);
  }

  private Object castValue(PartitionSpec spec, String columnName, String stringValue) {
    if (stringValue == null) {
      return null;
    }

    try {
      // Find the partition field type
      for (var field : spec.fields()) {
        if (field.name().equals(columnName)) {
          var fieldType = spec.schema().findType(field.sourceId());
          return castStringToExpectedColumnType(stringValue, fieldType);
        }
      }

      // If partition column not found, return the string value as fallback
      return stringValue;
    } catch (Exception e) {
      // If casting fails, return the string value as fallback
      return stringValue;
    }
  }

  private Object castStringToExpectedColumnType(String value, Type type) {
    if (value == null) {
      return null;
    }

    try {
      switch (type.typeId()) {
        case STRING:
          return value;
        case INTEGER:
          return Integer.parseInt(value);
        case LONG:
          return Long.parseLong(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case FLOAT:
          return Float.parseFloat(value);
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        case DECIMAL:
          // For decimal types, try to parse as double or return string
          try {
            return Double.parseDouble(value);
          } catch (NumberFormatException e) {
            return value;
          }
        default:
          // For other types, use string value
          return value;
      }
    } catch (NumberFormatException e) {
      // If parsing fails, return the original string value
      return value;
    }
  }
}
