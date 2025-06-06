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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import sample.Upper;

class SqlExecutorTest {

  @Test
  void testGetFunctionNameAndClass_FunctionClass() {
    var res = SqlExecutor.getFunctionNameAndClass(Upper.class);

    assertThat(res)
        .isPresent()
        .get()
        .satisfies(
            tuple -> {
              assertThat(tuple.f0).isEqualTo("upper");
              assertThat(tuple.f1).isEqualTo(Upper.class);
            });
  }

  @Test
  void testGetFunctionNameAndClass_NonFunctionClass() {
    var result = SqlExecutor.getFunctionNameAndClass(Object.class);

    assertThat(result).isEmpty();
  }
}
