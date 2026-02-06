/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.flinkrunner.stdlib.commons;

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;
import java.util.EnumSet;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

@AutoService(AutoRegisterSystemFunction.class)
public class to_append_stream extends ProcessTableFunction<Row>
    implements AutoRegisterSystemFunction {

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {

    return TypeInference.newBuilder()
        .staticArguments(
            StaticArgument.table(
                "input",
                Row.class,
                false,
                EnumSet.of(
                    StaticArgumentTrait.ROW_SEMANTIC_TABLE, StaticArgumentTrait.SUPPORT_UPDATES)))
        .outputTypeStrategy(
            ctx ->
                ctx.getTableSemantics(0)
                    .map(TableSemantics::dataType)
                    .or(() -> ctx.getArgumentDataTypes().stream().findFirst()))
        .build();
  }

  public void eval(Row input) {
    // keep +I and +U, drop -U and -D
    if (input.getKind() == RowKind.INSERT || input.getKind() == RowKind.UPDATE_AFTER) {

      // do not mutate the original row, it may be reused
      var out = Row.withPositions(input.getArity());
      for (var i = 0; i < input.getArity(); i++) {
        out.setField(i, input.getField(i));
      }

      // set INSERT explicitly
      out.setKind(RowKind.INSERT);
      collect(out);
    }
  }
}
