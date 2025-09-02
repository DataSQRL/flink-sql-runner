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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

class CosineDistanceTest {

  @Test
  public void testNullInputsReturnNull() {
    var function = new cosine_distance();

    assertThat(function.eval(null, null)).isNull();

    var vector = new FlinkVectorType(new double[] {1.0, 2.0, 3.0});
    assertThat(function.eval(null, vector)).isNull();
    assertThat(function.eval(vector, null)).isNull();
  }

  @Test
  public void testValidInputsReturnDistance() {
    var function = new cosine_distance();

    var vector1 = new FlinkVectorType(new double[] {1.0, 0.0, 0.0});
    var vector2 = new FlinkVectorType(new double[] {1.0, 0.0, 0.0});

    var result = function.eval(vector1, vector2);
    assertThat(result).isNotNull();
    assertThat(result).isCloseTo(0.0, within(1e-7));
  }
}
