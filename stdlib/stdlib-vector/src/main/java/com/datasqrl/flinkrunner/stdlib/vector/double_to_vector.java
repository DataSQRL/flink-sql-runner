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

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

/** Converts a double array to a vector */
@AutoService(AutoRegisterSystemFunction.class)
public class double_to_vector extends ScalarFunction implements AutoRegisterSystemFunction {

  private static final long serialVersionUID = 3241713829394296591L;

  public FlinkVectorType eval(double[] array) {
    return new FlinkVectorType(array);
  }
}
