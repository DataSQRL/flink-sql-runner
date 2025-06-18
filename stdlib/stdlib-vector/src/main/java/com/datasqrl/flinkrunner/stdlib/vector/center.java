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

import static com.datasqrl.flinkrunner.stdlib.vector.VectorFunctions.VEC_TO_DOUBLE;
import static com.datasqrl.flinkrunner.stdlib.vector.VectorFunctions.convert;

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * Aggregates vectors by computing the centroid, i.e. summing up all vectors and dividing the
 * resulting vector by the number of vectors.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class center extends AggregateFunction<FlinkVectorType, CenterAccumulator>
    implements AutoRegisterSystemFunction {

  private static final long serialVersionUID = 5356025229584544698L;

  @Override
  public CenterAccumulator createAccumulator() {
    return new CenterAccumulator();
  }

  @Override
  public FlinkVectorType getValue(CenterAccumulator acc) {
    if (acc.count == 0) {
      return null;
    } else {
      return convert(acc.get());
    }
  }

  public void accumulate(CenterAccumulator acc, FlinkVectorType vector) {
    acc.add(VEC_TO_DOUBLE.eval(vector));
  }

  public void retract(CenterAccumulator acc, FlinkVectorType vector) {
    acc.substract(VEC_TO_DOUBLE.eval(vector));
  }

  public void merge(CenterAccumulator acc, Iterable<CenterAccumulator> iter) {
    for (CenterAccumulator a : iter) {
      acc.addAll(a);
    }
  }

  public void resetAccumulator(CenterAccumulator acc) {
    acc.count = 0;
    acc.sum = null;
  }
}
