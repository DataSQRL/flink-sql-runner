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
package com.datasqrl.flinkrunner.functions.vector;

import static com.datasqrl.flinkrunner.functions.vector.VectorFunctions.VEC_TO_DOUBLE;

import com.datasqrl.flinkrunner.functions.AutoRegisterSystemFunction;
import com.datasqrl.flinkrunner.types.vector.FlinkVectorType;
import com.google.auto.service.AutoService;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.flink.table.functions.ScalarFunction;

/** Computes the euclidean distance between two vectors */
@AutoService(AutoRegisterSystemFunction.class)
public class euclidean_distance extends ScalarFunction implements AutoRegisterSystemFunction {

  private static final long serialVersionUID = 5193554802773750128L;

  public double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    // Create RealVectors from the input arrays
    RealVector vA = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorA), false);
    RealVector vB = new ArrayRealVector(VEC_TO_DOUBLE.eval(vectorB), false);
    return vA.getDistance(vB);
  }
}
