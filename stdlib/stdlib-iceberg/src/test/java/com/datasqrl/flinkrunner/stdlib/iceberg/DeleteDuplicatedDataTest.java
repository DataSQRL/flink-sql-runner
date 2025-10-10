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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.jupiter.api.Test;

class DeleteDuplicatedDataTest {

  @Test
  void testFunctionIsScalarFunction() {
    var function = new delete_duplicated_data();

    assertThat(function).isInstanceOf(ScalarFunction.class);
  }

  @Test
  void testEvalWithNullWarehouseReturnsFalse() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("partition1", "value1"), 1);

    boolean result = function.eval(null, "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullCatalogNameReturnsFalse() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("partition1", "value1"), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", null, "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullTableNameReturnsFalse() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("partition1", "value1"), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", null, 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullMaxTimeBucketReturnsFalse() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("partition1", "value1"), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", null, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullPartitionSetReturnsFalse() {
    var function = new delete_duplicated_data();

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, null);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithEmptyPartitionSetReturnsFalse() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithInconsistentPartitionKeysetReturnsFalse() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();

    // First partition map has "partition1"
    partitionSet.put(Map.of("partition1", "value1"), 1);

    // Second partition map has different keys - should fail validation
    partitionSet.put(Map.of("partition2", "value2"), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithConsistentPartitionKeysetsDoesNotThrow() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();

    // Both maps have the same keyset - should pass validation
    partitionSet.put(Map.of("partition1", "value1"), 1);
    partitionSet.put(Map.of("partition1", "value2"), 1);

    // Will return false because table doesn't exist, but validation should pass
    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithMultipleConsistentPartitionColumns() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();

    // Multiple columns, consistent keyset
    partitionSet.put(Map.of("col1", "val1", "col2", "val2"), 1);
    partitionSet.put(Map.of("col1", "val3", "col2", "val4"), 1);
    partitionSet.put(Map.of("col1", "val5", "col2", "val6"), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithEmptyPartitionMapsInSet() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();

    // Empty partition maps (for tables partitioned only by time bucket)
    partitionSet.put(Map.of(), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullDatabaseName() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("partition1", "value1"), 1);

    // Null database should be handled (defaults to "default_database" in CatalogUtils)
    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", null, "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithSinglePartitionMap() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("user_id", "12345"), 1);

    boolean result =
        function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", 1000L, partitionSet);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithHadoopCatalogType() {
    var function = new delete_duplicated_data();
    Map<Map<String, String>, Integer> partitionSet = new HashMap<>();
    partitionSet.put(Map.of("partition_col", "value"), 1);

    boolean result =
        function.eval(
            "/tmp/warehouse",
            "hadoop",
            "test_catalog",
            "test_db",
            "test_table",
            1000L,
            partitionSet);

    assertThat(result).isFalse();
  }
}
