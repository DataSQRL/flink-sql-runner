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

import java.util.HashMap;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Runs {@link hash_key} through the Flink planner, from SQL and from a compiled plan. */
class HashKeySqlTest {

  private static final TypeInformation<Row> NESTED_TYPE =
      Types.ROW_NAMED(new String[] {"x", "xs"}, Types.INT, Types.OBJECT_ARRAY(Types.INT));

  private StreamTableEnvironment tEnv;
  private StreamExecutionEnvironment env;

  @BeforeEach
  void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporarySystemFunction("hash_key", hash_key.class);
  }

  @Test
  @SneakyThrows
  void givenEventsFromStream_whenKeyedInSql_thenOnlyEqualRowsShareKeys() {
    var rows =
        List.of(
            event(0, 1, 23, "x", true),
            event(1, 12, 3, "x", true),
            event(2, 5, 5, null, false),
            event(3, 5, 5, "", false),
            event(4, 7, 7, "y", true),
            event(5, 7, 7, "y", true));
    var stream =
        env.fromData(
            rows,
            Types.ROW_NAMED(
                new String[] {"id", "a", "b", "note", "tags", "payload", "nested"},
                Types.INT,
                Types.INT,
                Types.INT,
                Types.STRING,
                Types.OBJECT_ARRAY(Types.INT),
                Types.PRIMITIVE_ARRAY(Types.BYTE),
                NESTED_TYPE));
    tEnv.createTemporaryView("events", tEnv.fromDataStream(stream));

    var keys = new HashMap<Integer, String>();
    try (var results =
        tEnv.executeSql("SELECT id, hash_key(a, b, note, tags, payload, nested) FROM events")
            .collect()) {
      results.forEachRemaining(row -> keys.put(row.<Integer>getFieldAs(0), row.getFieldAs(1)));
    }

    assertThat(keys).hasSize(rows.size());
    assertThat(keys.get(0)).as("1, 23 vs 12, 3").isNotEqualTo(keys.get(1));
    assertThat(keys.get(2)).as("NULL vs empty note").isNotEqualTo(keys.get(3));
    assertThat(keys.get(4)).as("replayed row with arrays").isEqualTo(keys.get(5));
    assertThat(keys.get(0))
        .as("planner evaluation matches direct evaluation on internal data")
        .isEqualTo(
            new hash_key(
                    List.of(
                        DataTypes.INT(),
                        DataTypes.INT(),
                        DataTypes.STRING(),
                        DataTypes.ARRAY(DataTypes.INT()),
                        DataTypes.BYTES(),
                        DataTypes.ROW(
                            DataTypes.FIELD("x", DataTypes.INT()),
                            DataTypes.FIELD("xs", DataTypes.ARRAY(DataTypes.INT())))))
                .eval(
                    1,
                    23,
                    StringData.fromString("x"),
                    new GenericArrayData(new Integer[] {1, 2}),
                    new byte[] {1, 2},
                    GenericRowData.of(1, new GenericArrayData(new Integer[] {3}))));
  }

  @Test
  @SneakyThrows
  void givenCompiledPlan_whenLoadedAndExecuted_thenKeysAreComputed() {
    tEnv.executeSql(
        """
        CREATE TABLE gen (
          a INT,
          tags ARRAY<INT>,
          payload BYTES,
          nested ROW<x INT, xs ARRAY<INT>>
        ) WITH ('connector' = 'datagen', 'number-of-rows' = '20')""");
    tEnv.executeSql("CREATE TABLE out_keys (k CHAR(32)) WITH ('connector' = 'blackhole')");

    var plan =
        tEnv.compilePlanSql(
                "INSERT INTO out_keys SELECT hash_key(a, tags, payload, nested) FROM gen")
            .asJsonString();

    assertThat(plan).contains("hash_key");
    tEnv.loadPlan(PlanReference.fromJsonString(plan)).execute().await();
  }

  @Test
  @SneakyThrows
  void givenNullLiteral_whenKeyedInSql_thenKeyMatchesTypedNull() {
    try (var results =
        tEnv.executeSql("SELECT hash_key(1, NULL), hash_key(1, CAST(NULL AS INT)), hash_key(NULL)")
            .collect()) {
      var row = results.next();
      assertThat(row.<String>getFieldAs(0)).isEqualTo(row.getFieldAs(1));
      assertThat(row.<String>getFieldAs(2)).hasSize(32);
    }
  }

  /** An event whose complex columns are new array instances with the same content each time. */
  private static Row event(int id, int a, int b, String note, boolean withComplexColumns) {
    if (!withComplexColumns) {
      return Row.of(id, a, b, note, null, null, null);
    }
    return Row.of(
        id, a, b, note, new Integer[] {1, 2}, new byte[] {1, 2}, Row.of(1, new Integer[] {3}));
  }
}
