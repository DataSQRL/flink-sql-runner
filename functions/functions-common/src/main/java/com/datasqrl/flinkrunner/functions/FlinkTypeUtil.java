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
package com.datasqrl.flinkrunner.functions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Singular;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.utils.AdaptedCallContext;

public class FlinkTypeUtil {

  public static TypeStrategy nullPreservingOutputStrategy(DataType outputType) {
    return callContext -> {
      var type = getFirstArgumentType(callContext);

      if (type.getLogicalType().isNullable()) {
        return Optional.of(outputType.nullable());
      }

      return Optional.of(outputType.notNull());
    };
  }

  public static TypeInference basicNullInference(DataType outputType, DataType inputType) {
    return TypeInference.newBuilder()
        .typedArguments(inputType)
        .outputTypeStrategy(nullPreservingOutputStrategy(outputType))
        .build();
  }

  public static TypeInference.Builder basicNullInferenceBuilder(
      DataType outputType, DataType inputType) {
    return TypeInference.newBuilder()
        .typedArguments(inputType)
        .outputTypeStrategy(nullPreservingOutputStrategy(outputType));
  }

  @SneakyThrows
  public static DataType getFirstArgumentType(CallContext callContext) {
    if (callContext instanceof AdaptedCallContext) {
      Field privateField = AdaptedCallContext.class.getDeclaredField("originalContext");
      privateField.setAccessible(true);
      CallContext originalContext = (CallContext) privateField.get(callContext);

      return originalContext.getArgumentDataTypes().get(0);
    } else {
      return callContext.getArgumentDataTypes().get(0);
    }
  }

  @Value
  @Builder
  public static class VariableArguments implements InputTypeStrategy {

    @Singular List<DataType> staticTypes;
    DataType variableType;
    int minVariableArguments;
    int maxVariableArguments;

    @Override
    public ArgumentCount getArgumentCount() {
      return new ArgumentCount() {
        @Override
        public boolean isValidCount(int count) {
          var variableCount = count - staticTypes.size();
          return variableCount >= minVariableArguments && variableCount <= maxVariableArguments;
        }

        @Override
        public Optional<Integer> getMinCount() {
          return Optional.of(staticTypes.size() + minVariableArguments);
        }

        @Override
        public Optional<Integer> getMaxCount() {
          return Optional.of(staticTypes.size() + maxVariableArguments);
        }
      };
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
        CallContext callContext, boolean throwOnFailure) {
      var argCount = callContext.getArgumentDataTypes().size();
      var varArgs = argCount - staticTypes.size();
      if (varArgs < 0 || varArgs < minVariableArguments || varArgs > maxVariableArguments) {
        return Optional.empty();
      }
      var result = new ArrayList<DataType>(argCount);
      result.addAll(staticTypes);
      for (var i = 0; i < varArgs; i++) {
        result.add(variableType);
      }
      return Optional.of(result);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
      List<Signature.Argument> arguments = new ArrayList<>(staticTypes.size() + 1);
      staticTypes.stream()
          .map(DataType::toString)
          .map(Signature.Argument::of)
          .forEach(arguments::add);
      arguments.add(Signature.Argument.of(variableType.toString() + "..."));
      return List.of(Signature.of(arguments));
    }
  }
}
