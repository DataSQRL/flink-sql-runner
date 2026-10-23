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
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.utils.DataTypeUtils;

/**
 * Computes a key from the given values: the SHA-256 digest of the values laid out as one row in
 * Flink's binary row format, truncated to 128 bits and hex encoded into 32 characters.
 *
 * <p>The binary row format is the representation Flink compares its own keys by. It covers the full
 * content of every value and marks null values explicitly, so distinct values only share a key
 * through a hash collision, and equal values, including arrays and binary strings, always share it.
 * Timestamps with a precision of up to 3 are hashed by their milliseconds, as Flink stores them.
 * Map and multiset values are hashed in their entry order. Multi-byte values are written in the
 * native byte order, so keys are the same on all little-endian platforms, such as x86-64 and
 * AArch64.
 *
 * <p>The function is specialized for the argument types of each call, so it can only be evaluated
 * by the Flink planner, or after {@link #specialize(SpecializedContext)}.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class hash_key extends ScalarFunction
    implements AutoRegisterSystemFunction, SpecializedFunction {

  private static final int KEY_BYTES = 16;
  private static final HexFormat HEX = HexFormat.of();

  /** Serializer of the arguments as one row, {@code null} until specialized for the call. */
  private final RowDataSerializer arguments;

  private transient MessageDigest digest;

  public hash_key() {
    this.arguments = null;
  }

  hash_key(List<DataType> argumentTypes) {
    this.arguments =
        new RowDataSerializer(
            argumentTypes.stream()
                .map(DataType::getLogicalType)
                // a NULL literal has no serializer, its value is always stored as a null field
                .map(type -> type.is(LogicalTypeRoot.NULL) ? new BooleanType() : type)
                .toArray(LogicalType[]::new));
  }

  @Override
  public UserDefinedFunction specialize(SpecializedContext context) {
    return new hash_key(context.getCallContext().getArgumentDataTypes());
  }

  @SneakyThrows
  public String eval(Object... values) {
    if (arguments == null) {
      throw new IllegalStateException(
          "hash_key must be specialized for its argument types before it is evaluated");
    }

    if (digest == null) {
      digest = MessageDigest.getInstance("SHA-256");
    }

    var row = arguments.toBinaryRow(GenericRowData.of(values));
    var bytes =
        BinarySegmentUtils.copyToBytes(row.getSegments(), row.getOffset(), row.getSizeInBytes());
    return HEX.formatHex(digest.digest(bytes), 0, KEY_BYTES);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .inputTypeStrategy(new InternalArgumentsStrategy())
        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.CHAR(KEY_BYTES * 2)))
        .build();
  }

  /** Accepts any number of arguments of any type and passes them as internal data structures. */
  private static final class InternalArgumentsStrategy implements InputTypeStrategy {

    @Override
    public ArgumentCount getArgumentCount() {
      return ConstantArgumentCount.any();
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
        CallContext callContext, boolean throwOnFailure) {
      return Optional.of(
          callContext.getArgumentDataTypes().stream()
              .map(DataTypeUtils::toInternalDataType)
              .toList());
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
      return List.of(Signature.of(Signature.Argument.ofGroupVarying("ANY")));
    }
  }
}
