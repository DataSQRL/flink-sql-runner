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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;

/**
 * Computes a key from the given values: the SHA-256 digest of their binary representation,
 * truncated to 128 bits and hex encoded into 32 characters.
 *
 * <p>The key covers the full content of every value. Each value is written with the serializer of
 * its internal data type, preceded by a null marker and its length, so distinct values only share a
 * key through a hash collision, and equal values, including arrays and binary strings, always share
 * it. Timestamps with a precision of up to 3 are hashed by their milliseconds, as Flink stores
 * them. Map and multiset values are hashed in their entry order.
 *
 * <p>The function is specialized for the argument types of each call, so it can only be evaluated
 * by the Flink planner, or after {@link #specialize(SpecializedContext)}.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class hash_key extends ScalarFunction
    implements AutoRegisterSystemFunction, SpecializedFunction {

  private static final int KEY_BYTES = 16;
  private static final byte NULL_MARKER = 0;
  private static final byte VALUE_MARKER = 1;
  private static final HexFormat HEX = HexFormat.of();

  /** Serializers of the arguments, {@code null} until specialized for the call. */
  private final TypeSerializer<Object>[] serializers;

  /** Arguments that are timestamps stored as milliseconds. */
  private final boolean[] compactTimestamps;

  private transient MessageDigest digest;
  private transient DataOutputSerializer buffer;

  public hash_key() {
    this.serializers = null;
    this.compactTimestamps = null;
  }

  @SuppressWarnings("unchecked")
  hash_key(List<DataType> argumentTypes) {
    var types = argumentTypes.stream().map(DataType::getLogicalType).toList();
    this.serializers =
        types.stream()
            // a NULL literal has no serializer, its value is always null
            .map(type -> type.is(LogicalTypeRoot.NULL) ? null : InternalSerializers.create(type))
            .toArray(TypeSerializer[]::new);
    this.compactTimestamps = new boolean[types.size()];
    for (var i = 0; i < types.size(); i++) {
      var type = types.get(i);
      compactTimestamps[i] =
          type.isAnyOf(
                  LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                  LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
              && TimestampData.isCompact(LogicalTypeChecks.getPrecision(type));
    }
  }

  @Override
  public UserDefinedFunction specialize(SpecializedContext context) {
    return new hash_key(context.getCallContext().getArgumentDataTypes());
  }

  @SneakyThrows
  public String eval(Object... values) {
    if (serializers == null) {
      throw new IllegalStateException(
          "hash_key must be specialized for its argument types before it is evaluated");
    }

    if (digest == null) {
      digest = MessageDigest.getInstance("SHA-256");
      buffer = new DataOutputSerializer(64);
    }

    for (var i = 0; i < values.length; i++) {
      var value = values[i];
      if (value == null) {
        digest.update(NULL_MARKER);
        continue;
      }

      if (compactTimestamps[i]
          && value instanceof TimestampData timestamp
          && timestamp.getNanoOfMillisecond() != 0) {
        // a source can deliver sub-millisecond precision that Flink drops when it stores the row
        value = TimestampData.fromEpochMillis(timestamp.getMillisecond());
      }

      buffer.clear();
      serializers[i].serialize(value, buffer);
      digest.update(VALUE_MARKER);
      updateLength(buffer.length());
      digest.update(buffer.getSharedBuffer(), 0, buffer.length());
    }

    return HEX.formatHex(digest.digest(), 0, KEY_BYTES);
  }

  private void updateLength(int length) {
    for (var shift = 24; shift >= 0; shift -= 8) {
      digest.update((byte) (length >>> shift));
    }
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
