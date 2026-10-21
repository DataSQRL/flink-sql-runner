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

import com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.OnConflictAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class SqrlPostgresTimestampConflictTest {

  private static final String TABLE = "events";
  private static final String[] FIELDS = {"id", "value", "event_time"};
  private static final Timestamp STORED_TIME = Timestamp.valueOf("2026-01-01 00:00:10");

  private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17");
  private static Connection conn;

  @BeforeAll
  static void startPostgres() throws SQLException {
    postgres.start();
    conn =
        DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @AfterAll
  static void stopPostgres() throws SQLException {
    conn.close();
    postgres.stop();
  }

  @BeforeEach
  void createTableWithStoredRow() throws SQLException {
    try (var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS \"events\"");
      stmt.execute(
          "CREATE TABLE \"events\" (\"id\" BIGINT PRIMARY KEY, \"value\" TEXT, \"event_time\" TIMESTAMP)");
    }
    upsert("stored", STORED_TIME);
  }

  @Test
  void givenStoredRow_whenUpsertWithNewerTimestamp_thenRowIsUpdated() throws SQLException {
    upsert("newer", Timestamp.valueOf("2026-01-01 00:00:11"));

    assertThat(storedValue()).isEqualTo("newer");
  }

  @Test
  void givenStoredRow_whenUpsertWithEqualTimestamp_thenRowIsUpdated() throws SQLException {
    upsert("equal", STORED_TIME);

    assertThat(storedValue()).isEqualTo("equal");
  }

  @Test
  void givenStoredRow_whenUpsertWithOlderTimestamp_thenRowIsKept() throws SQLException {
    upsert("older", Timestamp.valueOf("2026-01-01 00:00:09"));

    assertThat(storedValue()).isEqualTo("stored");
  }

  private static void upsert(String value, Timestamp eventTime) throws SQLException {
    var dialect = new SqrlPostgresDialect(OnConflictAction.TIMESTAMP, "event_time");
    var sql = dialect.getUpsertStatement(TABLE, FIELDS, new String[] {"id"}).orElseThrow();
    try (var stmt = FieldNamedPreparedStatement.prepareStatement(conn, sql, FIELDS)) {
      stmt.setLong(0, 1L);
      stmt.setString(1, value);
      stmt.setTimestamp(2, eventTime);
      stmt.addBatch();
      stmt.executeBatch();
    }
  }

  private static String storedValue() throws SQLException {
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT \"value\" FROM \"events\" WHERE \"id\" = 1")) {
      rs.next();
      return rs.getString(1);
    }
  }
}
