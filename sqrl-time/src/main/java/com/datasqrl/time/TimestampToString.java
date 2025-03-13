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
package com.datasqrl.time;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;
import java.time.Instant;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/** Converts the timestamp to an ISO timestamp string */
@AutoService(StandardLibraryFunction.class)
public class TimestampToString extends ScalarFunction implements StandardLibraryFunction {

  public String eval(Instant instant) {
    return instant.toString();
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return FlinkTypeUtil.basicNullInference(
        DataTypes.STRING(), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
  }
}
