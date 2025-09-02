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

/** A unuseful embedding function counts each character (modulo 256). Used for testing only. */
@AutoService(AutoRegisterSystemFunction.class)
public class ascii_text_test_embed extends ScalarFunction implements AutoRegisterSystemFunction {

  private static final long serialVersionUID = -6540980574407921967L;
  private static final int VECTOR_LENGTH = 256;

  public FlinkVectorType eval(String text) {
    if (text == null) {
      return null;
    }

    var vector = new double[256];
    for (char c : text.toCharArray()) {
      vector[c % VECTOR_LENGTH] += 1;
    }

    return new FlinkVectorType(vector);
  }
}
