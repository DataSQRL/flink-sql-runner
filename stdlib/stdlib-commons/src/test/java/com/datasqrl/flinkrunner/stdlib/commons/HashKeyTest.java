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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class HashKeyTest {

  private static final Instant TIME = Instant.parse("2026-10-23T10:38:53.201Z");

  @Test
  void givenDigitsThatConcatenateAlike_whenHashed_thenKeysDiffer() {
    var fn = hashKey(DataTypes.INT(), DataTypes.INT(), DataTypes.TIMESTAMP_LTZ(3));

    assertThat(fn.eval(1, 23, timestamp(TIME))).isNotEqualTo(fn.eval(12, 3, timestamp(TIME)));
    assertThat(fn.eval(1, 11, timestamp(TIME))).isNotEqualTo(fn.eval(11, 1, timestamp(TIME)));
  }

  @Test
  void givenNullZeroAndEmptyString_whenHashed_thenKeysDiffer() {
    var ints = hashKey(DataTypes.INT());
    var strings = hashKey(DataTypes.STRING());

    assertThat(ints.eval((Object) null)).isNotEqualTo(ints.eval(0));
    assertThat(strings.eval((Object) null)).isNotEqualTo(strings.eval(StringData.fromString("")));
  }

  @Test
  void givenValuesWithEqualJavaHashCodes_whenHashed_thenKeysDiffer() {
    var longs = hashKey(DataTypes.BIGINT());
    var strings = hashKey(DataTypes.STRING());
    var timestamps = hashKey(DataTypes.TIMESTAMP_LTZ(3));
    // Instant#hashCode is equal for these two, which are about 38 days apart
    var earlier = Instant.ofEpochSecond(TIME.getEpochSecond() - 3_294_336L, 959_000_000L);

    assertThat(longs.eval(0L)).isNotEqualTo(longs.eval(-1L));
    assertThat(strings.eval(StringData.fromString("Aa")))
        .isNotEqualTo(strings.eval(StringData.fromString("BB")));
    assertThat(timestamps.eval(timestamp(TIME))).isNotEqualTo(timestamps.eval(timestamp(earlier)));
  }

  @Test
  void givenNullInDifferentPositions_whenHashed_thenKeysDiffer() {
    var fn = hashKey(DataTypes.INT(), DataTypes.INT());

    assertThat(fn.eval(null, 1)).isNotEqualTo(fn.eval(1, null));
  }

  @Test
  void givenEqualArraysBinaryStringsAndRows_whenHashed_thenKeysMatch() {
    var fn =
        hashKey(
            DataTypes.ARRAY(DataTypes.INT()),
            DataTypes.BYTES(),
            DataTypes.ROW(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT())));

    var key =
        fn.eval(
            new GenericArrayData(new Integer[] {1, 2}),
            new byte[] {1, 2},
            GenericRowData.of(1, new GenericArrayData(new Integer[] {3})));

    assertThat(
            fn.eval(
                new GenericArrayData(new Integer[] {1, 2}),
                new byte[] {1, 2},
                GenericRowData.of(1, new GenericArrayData(new Integer[] {3}))))
        .isEqualTo(key);
  }

  @Test
  void givenGenericAndBinaryRepresentations_whenHashed_thenKeysMatch() {
    var arrays = hashKey(DataTypes.ARRAY(DataTypes.INT()));
    var rows = hashKey(DataTypes.ROW(DataTypes.INT(), DataTypes.STRING()));
    var genericArray = new GenericArrayData(new Integer[] {1, null, 3});
    var genericRow = GenericRowData.of(7, StringData.fromString("seven"));

    var binaryArray = new ArrayDataSerializer(new IntType()).toBinaryArray(genericArray);
    var binaryRow =
        new RowDataSerializer(new IntType(), DataTypes.STRING().getLogicalType())
            .toBinaryRow(genericRow);

    assertThat(arrays.eval(binaryArray)).isEqualTo(arrays.eval(genericArray));
    assertThat(rows.eval(binaryRow)).isEqualTo(rows.eval(genericRow));
  }

  @Test
  @SneakyThrows
  void givenSerializedFunction_whenHashedAfterDeserialization_thenKeysMatch() {
    var fn = hashKey(DataTypes.ARRAY(DataTypes.INT()), DataTypes.STRING());
    var copy = InstantiationUtil.clone(fn);

    assertThat(copy.eval(new GenericArrayData(new Integer[] {1, 2}), StringData.fromString("a")))
        .isEqualTo(fn.eval(new GenericArrayData(new Integer[] {1, 2}), StringData.fromString("a")));
  }

  @Test
  void givenFixedValues_whenHashed_thenKeyIsStable() {
    // Keys are stored as primary keys, so the encoding must not change between versions
    var fn = hashKey(DataTypes.INT(), DataTypes.STRING(), DataTypes.TIMESTAMP_LTZ(3));

    assertThat(fn.eval(1, StringData.fromString("a"), timestamp(TIME)))
        .isEqualTo("84e1f0fa9c16baae83a7ab8759a57269");
  }

  @Test
  void givenUnspecializedFunction_whenEvaluated_thenFails() {
    var fn = new hash_key();

    assertThatThrownBy(() -> fn.eval(1))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("specialized");
  }

  private static hash_key hashKey(DataType... argumentTypes) {
    return new hash_key(List.of(argumentTypes));
  }

  private static TimestampData timestamp(Instant instant) {
    return TimestampData.fromInstant(instant);
  }
}
