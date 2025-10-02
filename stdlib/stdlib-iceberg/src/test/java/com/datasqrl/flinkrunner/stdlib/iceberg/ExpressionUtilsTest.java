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

import org.apache.iceberg.expressions.Expression;
import org.junit.jupiter.api.Test;

class ExpressionUtilsTest {

  @Test
  void testBuildEqualsListWithSingleValue() {
    Object[] values = new Object[] {"partition1"};
    Expression result = ExpressionUtils.buildEqualsList("partition_col", values);

    assertThat(result).isNotNull();
    assertThat(result.toString()).contains("partition_col");
    assertThat(result.toString()).contains("partition1");
  }

  @Test
  void testBuildEqualsListWithMultipleValues() {
    Object[] values = new Object[] {"partition1", "partition2", "partition3"};
    Expression result = ExpressionUtils.buildEqualsList("partition_col", values);

    assertThat(result).isNotNull();
    assertThat(result.toString()).contains("partition_col");
    assertThat(result.toString()).contains("partition1");
    assertThat(result.toString()).contains("partition2");
    assertThat(result.toString()).contains("partition3");
  }

  @Test
  void testBuildEqualsListWithNumericValues() {
    Object[] values = new Object[] {1L, 2L, 3L};
    Expression result = ExpressionUtils.buildEqualsList("partition_id", values);

    assertThat(result).isNotNull();
    assertThat(result.toString()).contains("partition_id");
  }

  @Test
  void testBuildEqualsListWithNullArrayThrowsException() {
    assertThatThrownBy(() -> ExpressionUtils.buildEqualsList("partition_col", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("vals is marked non-null but is null");
  }

  @Test
  void testBuildEqualsListWithEmptyArrayThrowsException() {
    Object[] emptyValues = new Object[] {};
    assertThatThrownBy(() -> ExpressionUtils.buildEqualsList("partition_col", emptyValues))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Partition values must not be null or empty");
  }

  @Test
  void testBuildPartitionDelete() {
    Object[] partitionIds = new Object[] {"partition1", "partition2"};
    Long maxTimeBucket = 1000L;

    Expression result =
        ExpressionUtils.buildPartitionDelete("partition_col", partitionIds, maxTimeBucket);

    assertThat(result).isNotNull();
    String resultStr = result.toString();
    assertThat(resultStr).contains("time_bucket");
    assertThat(resultStr).contains("partition_col");
    assertThat(resultStr).contains("1000");
  }

  @Test
  void testBuildPartitionDeleteWithSinglePartition() {
    Object[] partitionIds = new Object[] {123L};
    Long maxTimeBucket = 500L;

    Expression result =
        ExpressionUtils.buildPartitionDelete("partition_id", partitionIds, maxTimeBucket);

    assertThat(result).isNotNull();
    assertThat(result.toString()).contains("time_bucket");
    assertThat(result.toString()).contains("partition_id");
    assertThat(result.toString()).contains("500");
  }

  @Test
  void testBuildPartitionDeleteWithMultiplePartitions() {
    Object[] partitionIds = new Object[] {1L, 2L, 3L, 4L, 5L};
    Long maxTimeBucket = 2000L;

    Expression result = ExpressionUtils.buildPartitionDelete("id", partitionIds, maxTimeBucket);

    assertThat(result).isNotNull();
    assertThat(result.toString()).contains("time_bucket");
    assertThat(result.toString()).contains("2000");
  }
}
