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
package com.datasqrl.json;

import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;

/** For a given JSON object, checks whether the provided JSON path exists */
@AutoService(StandardLibraryFunction.class)
public class JsonExists extends ScalarFunction implements StandardLibraryFunction {

  public Boolean eval(FlinkJsonType json, String path) {
    if (json == null) {
      return null;
    }
    try {
      return SqlJsonUtils.jsonExists(json.json.toString(), path);
    } catch (Exception e) {
      return false;
    }
  }
}
