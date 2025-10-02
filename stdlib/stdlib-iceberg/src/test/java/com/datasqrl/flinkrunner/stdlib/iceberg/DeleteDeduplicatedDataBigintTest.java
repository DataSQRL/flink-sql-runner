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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.junit.jupiter.api.Test;

class DeleteDeduplicatedDataBigintTest {

  @Test
  void testEvalWithValidInputsReturnsFalseWhenTableNotFound() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();
    partitionIds.put(1L, 1);
    partitionIds.put(2L, 2);

    assertThatThrownBy(
            () ->
                function.eval(
                    "/tmp/warehouse",
                    "hadoop",
                    "test_catalog",
                    "test_db",
                    "nonexistent_table",
                    "partition_id",
                    1000L,
                    partitionIds))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: test_db.nonexistent_table");
  }

  @Test
  void testEvalWithNullWarehouseReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();
    partitionIds.put(1L, 1);

    boolean result =
        function.eval(
            null, "hadoop", "catalog", "db", "table", "partition_id", 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullCatalogNameReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();
    partitionIds.put(1L, 1);

    boolean result =
        function.eval(
            "/tmp/warehouse", "hadoop", null, "db", "table", "partition_id", 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullTableNameReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();
    partitionIds.put(1L, 1);

    boolean result =
        function.eval(
            "/tmp/warehouse", "hadoop", "catalog", "db", null, "partition_id", 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullPartitionColReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();
    partitionIds.put(1L, 1);

    boolean result =
        function.eval(
            "/tmp/warehouse", "hadoop", "catalog", "db", "table", null, 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullMaxTimeBucketReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();
    partitionIds.put(1L, 1);

    boolean result =
        function.eval(
            "/tmp/warehouse",
            "hadoop",
            "catalog",
            "db",
            "table",
            "partition_id",
            null,
            partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithNullPartitionIdsReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();

    boolean result =
        function.eval(
            "/tmp/warehouse", "hadoop", "catalog", "db", "table", "partition_id", 1000L, null);

    assertThat(result).isFalse();
  }

  @Test
  void testEvalWithEmptyPartitionIdsReturnsFalse() {
    var function = new delete_deduplicated_data_bigint();
    Map<Long, Integer> partitionIds = new HashMap<>();

    boolean result =
        function.eval(
            "/tmp/warehouse",
            "hadoop",
            "catalog",
            "db",
            "table",
            "partition_id",
            1000L,
            partitionIds);

    assertThat(result).isFalse();
  }
}
