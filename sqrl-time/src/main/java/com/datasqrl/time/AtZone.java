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
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

/** Returns the timestamp at the given timezone. */
public class AtZone extends ScalarFunction {

  public ZonedDateTime eval(Instant instant, String zoneId) {
    return instant.atZone(ZoneId.of(zoneId));
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .typedArguments(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.STRING())
        .outputTypeStrategy(
            callContext -> {
              DataType type = FlinkTypeUtil.getFirstArgumentType(callContext);
              if (type.getLogicalType().isNullable()) {
                return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
              }

              return Optional.of(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3));
            })
        .build();
  }
}
