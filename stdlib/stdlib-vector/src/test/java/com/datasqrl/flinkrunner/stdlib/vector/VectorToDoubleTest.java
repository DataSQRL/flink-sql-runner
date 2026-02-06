/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class VectorToDoubleTest {

  @Test
  public void testNullInputReturnsNull() {
    var function = new vector_to_double();
    var result = function.eval(null);

    assertThat(result).isNull();
  }

  @Test
  public void testValidInputReturnsArray() {
    var function = new vector_to_double();
    var input = new FlinkVectorType(new double[] {1.0, 2.0, 3.0});
    var result = function.eval(input);

    assertThat(result).isNotNull();
    assertThat(result).containsExactly(1.0, 2.0, 3.0);
  }
}
