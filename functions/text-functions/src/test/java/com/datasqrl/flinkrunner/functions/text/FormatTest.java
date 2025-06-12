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
package com.datasqrl.flinkrunner.functions.text;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FormatTest {

  format underTest = new format();

  @Test
  public void testFormat() {
    var format = "Hello, %s";
    assertThat(underTest.eval(format, "World")).isEqualTo("Hello, World");
    format = "Count: %s, %s, %s, %s";
    assertThat(underTest.eval(format, "1", "2", "3", "4")).isEqualTo("Count: 1, 2, 3, 4");
  }
}
