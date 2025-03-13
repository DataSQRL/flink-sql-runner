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
import com.datasqrl.function.FlinkTypeUtil.VariableArguments;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/** Parses a timestamp from an ISO timestamp string. */
@Slf4j
@AutoService(StandardLibraryFunction.class)
public class ParseTimestamp extends ScalarFunction implements StandardLibraryFunction {

  public Instant eval(String s) {
    return Instant.parse(s);
  }

  public Instant eval(String s, String format) {
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format, Locale.US);
    try {
      return LocalDateTime.parse(s, formatter).atZone(ZoneId.systemDefault()).toInstant();
    } catch (Exception e) {
      log.warn(e.getMessage());
      return null;
    }
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .inputTypeStrategy(
            VariableArguments.builder()
                .staticType(DataTypes.STRING())
                .variableType(DataTypes.STRING())
                .minVariableArguments(0)
                .maxVariableArguments(1)
                .build())
        .outputTypeStrategy(
            FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
        .build();
  }
}
