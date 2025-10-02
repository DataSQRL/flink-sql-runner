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

class DeduplicatedDataDeleterTest {

  private static class TestDeleter extends DeduplicatedDataDeleter {
    // Concrete implementation for testing
  }

  @Test
  void testDeleteDeduplicatedDataWithNullWarehouse() {
    var deleter = new TestDeleter();
    Object[] partitionIds = new Object[] {"partition1"};

    boolean result =
        deleter.deleteDeduplicatedData(
            null, "hadoop", "catalog", "db", "table", "partition_col", 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testDeleteDeduplicatedDataWithNullCatalogName() {
    var deleter = new TestDeleter();
    Object[] partitionIds = new Object[] {"partition1"};

    boolean result =
        deleter.deleteDeduplicatedData(
            "warehouse", "hadoop", null, "db", "table", "partition_col", 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testDeleteDeduplicatedDataWithNullTableName() {
    var deleter = new TestDeleter();
    Object[] partitionIds = new Object[] {"partition1"};

    boolean result =
        deleter.deleteDeduplicatedData(
            "warehouse", "hadoop", "catalog", "db", null, "partition_col", 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testDeleteDeduplicatedDataWithNullPartitionCol() {
    var deleter = new TestDeleter();
    Object[] partitionIds = new Object[] {"partition1"};

    boolean result =
        deleter.deleteDeduplicatedData(
            "warehouse", "hadoop", "catalog", "db", "table", null, 1000L, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testDeleteDeduplicatedDataWithNullMaxTimeBucket() {
    var deleter = new TestDeleter();
    Object[] partitionIds = new Object[] {"partition1"};

    boolean result =
        deleter.deleteDeduplicatedData(
            "warehouse", "hadoop", "catalog", "db", "table", "partition_col", null, partitionIds);

    assertThat(result).isFalse();
  }

  @Test
  void testDeleteDeduplicatedDataWithNullPartitionIds() {
    var deleter = new TestDeleter();

    boolean result =
        deleter.deleteDeduplicatedData(
            "warehouse", "hadoop", "catalog", "db", "table", "partition_col", 1000L, null);

    assertThat(result).isFalse();
  }

  @Test
  void testDeleteDeduplicatedDataWithEmptyPartitionIds() {
    var deleter = new TestDeleter();
    Object[] partitionIds = new Object[] {};

    boolean result =
        deleter.deleteDeduplicatedData(
            "warehouse", "hadoop", "catalog", "db", "table", "partition_col", 1000L, partitionIds);

    assertThat(result).isFalse();
  }
}
