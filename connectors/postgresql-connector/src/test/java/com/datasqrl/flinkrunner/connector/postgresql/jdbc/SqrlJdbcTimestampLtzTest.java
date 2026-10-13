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
package com.datasqrl.flinkrunner.connector.postgresql.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.TimeZone;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Verifies that {@code TIMESTAMP_LTZ} values are written to and read from Postgres {@code
 * timestamptz} columns without being shifted by the JVM default time zone.
 */
@ExtendWith(MiniClusterExtension.class)
class SqrlJdbcTimestampLtzTest {

  private static final String POSTGRES_IMG = "postgres:17";
  private static final long EPOCH_MILLIS = 1760357486575L; // 2025-10-13T12:11:26.575Z
  private static final Instant EXPECTED = Instant.ofEpochMilli(EPOCH_MILLIS);

  private TimeZone originalTimeZone;

  @BeforeEach
  void saveTimeZone() {
    originalTimeZone = TimeZone.getDefault();
  }

  @AfterEach
  void restoreTimeZone() {
    TimeZone.setDefault(originalTimeZone);
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "America/Los_Angeles", "Asia/Kolkata"})
  void givenJvmTimeZone_whenWriteTimestampLtzToPostgres_thenInstantIsPreserved(String timeZone)
      throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone));

    try (var postgres = new PostgreSQLContainer(POSTGRES_IMG)) {
      postgres.start();
      try (var conn =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          var stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE TABLE ts_table (id BIGINT, event_time TIMESTAMPTZ)");
      }

      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      var settings = EnvironmentSettings.newInstance().inStreamingMode().build();
      var tEnv = StreamTableEnvironment.create(env, settings);
      tEnv.executeSql(
          """
          CREATE TABLE ts_table (
            id BIGINT,
            event_time TIMESTAMP_LTZ(3)
          ) WITH (
            'connector' = 'jdbc-sqrl',
            'url' = '%s',
            'table-name' = 'ts_table',
            'username' = '%s',
            'password' = '%s'
          )"""
              .formatted(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()));

      tEnv.executeSql(
              "INSERT INTO ts_table SELECT 1, TO_TIMESTAMP_LTZ(%d, 3)".formatted(EPOCH_MILLIS))
          .await();

      try (var conn =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          var stmt = conn.createStatement();
          var rs = stmt.executeQuery("SELECT event_time FROM ts_table WHERE id = 1")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getObject(1, OffsetDateTime.class).toInstant()).isEqualTo(EXPECTED);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "America/Los_Angeles", "Asia/Kolkata"})
  void givenJvmTimeZone_whenReadTimestampLtzFromJdbc_thenInstantIsPreserved(String timeZone)
      throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
    var type = new LocalZonedTimestampType(3);
    var converter = new SqrlPostgresRowConverter(RowType.of(type));

    var result = converter.createInternalConverter(type).deserialize(Timestamp.from(EXPECTED));

    assertThat(((TimestampData) result).toInstant()).isEqualTo(EXPECTED);
  }
}
