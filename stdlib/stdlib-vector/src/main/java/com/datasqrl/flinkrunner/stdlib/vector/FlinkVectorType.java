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
package com.datasqrl.flinkrunner.stdlib.vector;

import java.util.Arrays;
import org.apache.flink.table.annotation.DataTypeHint;

@DataTypeHint(
    value = "RAW",
    bridgedTo = FlinkVectorType.class,
    rawSerializer = FlinkVectorTypeSerializer.class)
public class FlinkVectorType {

  public double[] value;

  public FlinkVectorType(double[] value) {
    this.value = value;
  }

  public double[] getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof FlinkVectorType)) return false;
    var other = (FlinkVectorType) obj;
    return Arrays.equals(value, other.value);
  }
}
