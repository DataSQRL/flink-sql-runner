/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.types.vector.functions;

// import com.google.common.base.Preconditions;

// mutable accumulator of structured type for the aggregate function
public class CenterAccumulator {

  public double[] sum = null;
  public int count = 0;

  public synchronized void add(double[] values) {
    if (count == 0) {
      sum = values.clone();
      count = 1;
    } else {
      //      Preconditions.checkArgument(values.length == sum.length);
      for (var i = 0; i < values.length; i++) {
        sum[i] += values[i];
      }
      count++;
    }
  }

  public synchronized void addAll(CenterAccumulator other) {
    if (other.count == 0) {
      return;
    }
    if (this.count == 0) {
      this.sum = new double[other.sum.length];
    }
    //    Preconditions.checkArgument(this.sum.length == other.sum.length);
    for (var i = 0; i < other.sum.length; i++) {
      this.sum[i] += other.sum[i];
    }
    this.count += other.count;
  }

  public double[] get() {
    //    Preconditions.checkArgument(count > 0);
    var result = new double[sum.length];
    for (var i = 0; i < sum.length; i++) {
      result[i] = sum[i] / count;
    }
    return result;
  }

  public synchronized void substract(double[] values) {
    //    Preconditions.checkArgument(values.length == sum.length);
    for (var i = 0; i < values.length; i++) {
      sum[i] -= values[i];
    }
    count--;
  }
}
