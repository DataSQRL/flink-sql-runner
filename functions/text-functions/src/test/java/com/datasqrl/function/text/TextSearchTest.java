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
package com.datasqrl.function.text;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TextSearchTest {

  private text_search underTest = new text_search();

  @Test
  public void testSearch() {
    assertEquals(1.0 / 2, underTest.eval("Hello World", "hello john"));
    assertEquals(1.0 / 2, underTest.eval("Hello World", "what a world we live in, john"));
    assertEquals(1.0, underTest.eval("Hello World", "what a world we live in, john! Hello john"));
    assertEquals(
        2.0 / 3, underTest.eval("one two THREE", "we are counting", "one two four five six"));
    assertEquals(
        1.0,
        underTest.eval(
            "one two THREE", "we are counting", "one two four five six", "three forty fiv"));
    assertEquals(
        0, underTest.eval("one two THREE", "what a world we live in, john!", " Hello john"));
  }
}
