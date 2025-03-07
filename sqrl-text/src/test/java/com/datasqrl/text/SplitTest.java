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
package com.datasqrl.text;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class SplitTest {

  private final Split splitFunction = new Split();

  // Test for "Returns an array of substrings by splitting the input string based on the given
  // delimiter."
  @Test
  public void testSplitWithDelimiter() {
    String[] result = splitFunction.eval("apple,banana,cherry", ",");
    assertArrayEquals(new String[] {"apple", "banana", "cherry"}, result);
  }

  // Test for "If the delimiter is not found in the string, the original string is returned as the
  // only element in the array."
  @Test
  public void testSplitWithNoDelimiterInString() {
    String[] result = splitFunction.eval("apple", ",");
    assertArrayEquals(new String[] {"apple"}, result);
  }

  // Test for "If the delimiter is empty, every character in the string is split."
  @Test
  public void testSplitWithEmptyDelimiter() {
    String[] result = splitFunction.eval("apple", "");
    assertArrayEquals(new String[] {"a", "p", "p", "l", "e"}, result);
  }

  // Test for "If the string is null, a null value is returned."
  @Test
  public void testSplitWithNullText() {
    String[] result = splitFunction.eval(null, ",");
    assertNull(result);
  }

  // Test for "If the delimiter is null, a null value is returned."
  @Test
  public void testSplitWithNullDelimiter() {
    String[] result = splitFunction.eval("apple,banana,cherry", null);
    assertNull(result);
  }

  // Test for "If the delimiter is found at the beginning of the string, an empty string is added to
  // the array."
  @Test
  public void testSplitWithDelimiterAtBeginning() {
    String[] result = splitFunction.eval(",apple,banana,cherry", ",");
    assertArrayEquals(new String[] {"", "apple", "banana", "cherry"}, result);
  }

  // Test for "If the delimiter is found at the end of the string, an empty string is added to the
  // array."
  @Test
  public void testSplitWithDelimiterAtEnd() {
    String[] result = splitFunction.eval("apple,banana,cherry,", ",");
    assertArrayEquals(new String[] {"apple", "banana", "cherry", ""}, result);
  }

  // Test for "If there are contiguous delimiters, then an empty string is added to the array."
  @Test
  public void testSplitWithContiguousDelimiters() {
    String[] result = splitFunction.eval("apple,,banana,cherry", ",");
    assertArrayEquals(new String[] {"apple", "", "banana", "cherry"}, result);
  }
}
