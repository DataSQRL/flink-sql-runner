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
package sample;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts the input string to uppercase. If the input string is null, a null value is returned.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class Upper extends ScalarFunction implements AutoRegisterSystemFunction {

  public String eval(String text) {
    if (text == null) {
      return null;
    }

    return text.toUpperCase();
  }
}
