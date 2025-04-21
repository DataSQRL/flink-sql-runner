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

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.datasqrl.types.vector.FlinkVectorType;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

/** A unuseful embedding function counts each character (modulo 256). Used for testing only. */
@AutoService(AutoRegisterSystemFunction.class)
public class AsciiTextTestEmbed extends ScalarFunction implements AutoRegisterSystemFunction {

  private static final int VECTOR_LENGTH = 256;

  public FlinkVectorType eval(String text) {
    var vector = new double[256];
    for (char c : text.toCharArray()) {
      vector[c % VECTOR_LENGTH] += 1;
    }
    return new FlinkVectorType(vector);
  }
}
