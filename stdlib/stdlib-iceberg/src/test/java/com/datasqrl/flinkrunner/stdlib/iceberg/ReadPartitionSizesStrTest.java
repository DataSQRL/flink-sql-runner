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

import org.junit.jupiter.api.Test;

class ReadPartitionSizesStrTest {

  @Test
  void testGetUnpartitionedIdReturnsUnpartitionedString() {
    var function = new read_partition_sizes_str();

    String result = function.getUnpartitionedId();

    assertThat(result).isEqualTo("__UNPARTITIONED__");
  }

  @Test
  void testEvalWithNullWarehouseDoesNotThrow() {
    var function = new read_partition_sizes_str();

    // Should not throw, just return without collecting any rows
    function.eval(null, "hadoop", "catalog", "db", "table", "partition_col");

    // No assertion needed - test passes if no exception is thrown
  }

  @Test
  void testEvalWithNullCatalogNameDoesNotThrow() {
    var function = new read_partition_sizes_str();

    function.eval("/tmp/warehouse", "hadoop", null, "db", "table", "partition_col");

    // No assertion needed - test passes if no exception is thrown
  }

  @Test
  void testEvalWithNullTableNameDoesNotThrow() {
    var function = new read_partition_sizes_str();

    function.eval("/tmp/warehouse", "hadoop", "catalog", "db", null, "partition_col");

    // No assertion needed - test passes if no exception is thrown
  }

  @Test
  void testEvalWithNullPartitionColDoesNotThrow() {
    var function = new read_partition_sizes_str();

    function.eval("/tmp/warehouse", "hadoop", "catalog", "db", "table", null);

    // No assertion needed - test passes if no exception is thrown
  }

  @Test
  void testEvalWithAllNullParametersDoesNotThrow() {
    var function = new read_partition_sizes_str();

    function.eval(null, null, null, null, null, null);

    // No assertion needed - test passes if no exception is thrown
  }

  @Test
  void testFunctionIsTableFunction() {
    var function = new read_partition_sizes_str();

    // Verify it's a proper TableFunction subclass
    assertThat(function).isInstanceOf(org.apache.flink.table.functions.TableFunction.class);
  }
}
