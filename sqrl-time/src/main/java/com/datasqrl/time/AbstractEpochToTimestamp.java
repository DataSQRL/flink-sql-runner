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
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

@AllArgsConstructor
public class AbstractEpochToTimestamp extends ScalarFunction {

  boolean isMilli;

  public Instant eval(Long l) {
    if (isMilli) {
      return Instant.ofEpochMilli(l.longValue());
    } else {
      return Instant.ofEpochSecond(l.longValue());
    }
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return FlinkTypeUtil.basicNullInference(
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.BIGINT());
  }
  //
  //  @Override
  //  public String getDocumentation() {
  //    Instant inst = DEFAULT_DOC_TIMESTAMP.truncatedTo(ChronoUnit.SECONDS);
  //    long epoch = inst.toEpochMilli() / (isMilli ? 1 : 1000);
  //    String functionCall = String.format("%s(%s)", getFunctionName(), epoch);
  //    String result = this.eval(epoch).toString();
  //    return String.format(
  //        "Converts the epoch timestamp in %s to the corresponding timestamp.<br />E.g. `%s`
  // returns the timestamp `%s`",
  //        isMilli ? "milliseconds" : "seconds", functionCall, result);
  //  }

}
