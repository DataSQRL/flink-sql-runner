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

import com.datasqrl.flinkrunner.functions.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Returns an array of substrings by splitting the input string based on the given delimiter. If the
 * delimiter is not found in the string, the original string is returned as the only element in the
 * array. If the delimiter is empty, every character in the string is split. If the string or
 * delimiter is null, a null value is returned. If the delimiter is found at the beginning or end of
 * the string, or there are contiguous delimiters, then an empty string is added to the array.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class split extends ScalarFunction implements AutoRegisterSystemFunction {

  public String[] eval(String text, String delimiter) {
    if (text == null || delimiter == null) {
      return null;
    }

    if (delimiter.isEmpty()) {
      return text.split("");
    }

    return text.split(delimiter, -1);
  }
}
