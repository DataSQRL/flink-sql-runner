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
package com.datasqrl.flinkrunner.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializerSnapshot;
import com.datasqrl.flinkrunner.stdlib.json.to_jsonb;
import java.io.IOException;
import java.sql.DriverManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;

@ExtendWith(MiniClusterExtension.class)
@Slf4j
public class FlinkJdbcTest {

  public static void main(String[] args) throws IOException {
    var input =
        new DataInputDeserializer(
            EncodingUtils.decodeBase64ToBytes(
                "ADFjb20uZGF0YXNxcmwuanNvbi5GbGlua0pzb25UeXBlU2VyaWFsaXplclNuYXBzaG90AAAAAQApY29tLmRhdGFzcXJsLmpzb24uRmxpbmtKc29uVHlwZVNlcmlhbGl6ZXI="));

    log.debug(input.readUTF());
    log.debug("{}", input.readInt());
    log.debug(input.readUTF());

    var output = new DataOutputSerializer(91);
    var snapshot = new FlinkJsonTypeSerializerSnapshot();
    output.writeUTF(snapshot.getClass().getName());
    output.writeInt(snapshot.getCurrentVersion());
    snapshot.writeSnapshot(output);

    log.debug(EncodingUtils.encodeBytesToBase64(output.getSharedBuffer()));
  }

  @Test
  public void testFlinkWithPostgres() throws Exception {
    // Start PostgreSQL container
    try (PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:14")) {
      postgres.start();
      // Establish a connection and create the PostgreSQL table
      try (var conn =
              DriverManager.getConnection(
                  postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
          var stmt = conn.createStatement()) {
        var createTableSQL = "CREATE TABLE test_table (\n" + "\"arrayOfRows\" JSONB\n" + ")";
        stmt.executeUpdate(createTableSQL);
      }

      // Set up Flink environment
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      var tableEnv = StreamTableEnvironment.create(env);

      // Define the schema
      var createSourceTable =
          "CREATE TABLE datagen_source ("
              + "arrayOfRows ARRAY<ROW<field1 INT, field2 STRING>>"
              + ") WITH ("
              + "'connector' = 'datagen',"
              + "'number-of-rows' = '10'"
              + ")";

      var createSinkTable =
          "CREATE TABLE jdbc_sink ("
              + "    arrayOfRows RAW('com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType', 'AERjb20uZGF0YXNxcmwuZmxpbmtydW5uZXIuc3RkbGliLmpzb24uRmxpbmtKc29uVHlwZVNlcmlhbGl6ZXJTbmFwc2hvdAAAAAMAAAAAAAAAAAAAAAAAAAAAAA==') "
              + ") WITH ("
              + "    'connector' = 'jdbc-sqrl', "
              + "    'url' = '"
              + postgres.getJdbcUrl()
              + "', "
              + "    'table-name' = 'test_table', "
              + "    'username' = '"
              + postgres.getUsername()
              + "', "
              + "    'password' = '"
              + postgres.getPassword()
              + "'"
              + ")";
      var jsonClassName = to_jsonb.class.getName();
      // Register tables in the environment
      tableEnv.executeSql(
          "CREATE TEMPORARY FUNCTION IF NOT EXISTS `to_json` AS '"
              + jsonClassName
              + "' LANGUAGE JAVA");
      tableEnv.executeSql(createSourceTable);
      tableEnv.executeSql(createSinkTable);

      // Set up a simple Flink job
      var tableResult =
          tableEnv.executeSql(
              "INSERT INTO jdbc_sink SELECT to_json(arrayOfRows) AS arrayOfRows FROM datagen_source");
      tableResult.print();

      assertThat(tableResult.getResultKind()).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT);
    }
  }

  @Test
  public void testWriteAndReadToPostgres() throws Exception {
    try (PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:14")) {
      postgresContainer.start();
      try (var conn =
              DriverManager.getConnection(
                  postgresContainer.getJdbcUrl(),
                  postgresContainer.getUsername(),
                  postgresContainer.getPassword());
          var stmt = conn.createStatement()) {
        var createTableSQL = "CREATE TABLE test_table (" + "id BIGINT, name VARCHAR" + ")";
        stmt.executeUpdate(createTableSQL);
      }

      // Set up Flink mini cluster environment
      var env = StreamExecutionEnvironment.getExecutionEnvironment();
      var settings = EnvironmentSettings.newInstance().inStreamingMode().build();
      var tEnv = StreamTableEnvironment.create(env, settings);

      // Create a PostgreSQL table using the Table API
      tEnv.executeSql(
          "CREATE TABLE test_table ("
              + "id BIGINT,"
              + "name STRING"
              + ") WITH ("
              + "'connector' = 'jdbc',"
              + "'url' = '"
              + postgresContainer.getJdbcUrl()
              + "',"
              + "'table-name' = 'test_table',"
              + "'username' = '"
              + postgresContainer.getUsername()
              + "',"
              + "'password' = '"
              + postgresContainer.getPassword()
              + "'"
              + ")");

      // Create a DataGen source to generate 10 rows of data
      tEnv.executeSql(
          "CREATE TABLE datagen_source ("
              + "id BIGINT,"
              + "name STRING"
              + ") WITH ("
              + "'connector' = 'datagen',"
              + "'rows-per-second' = '1',"
              + "'fields.id.kind' = 'sequence',"
              + "'fields.id.start' = '1',"
              + "'fields.id.end' = '10',"
              + "'fields.name.length' = '10'"
              + ")");

      // Insert data from the DataGen source into the PostgreSQL table
      tEnv.executeSql("INSERT INTO test_table SELECT * FROM datagen_source").await();

      // Verify the data has been inserted by querying the PostgreSQL database directly
      var connection = postgresContainer.createConnection("");
      var statement = connection.createStatement();
      var resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_table");

      var count = 0;
      if (resultSet.next()) {
        count = resultSet.getInt(1);
      }

      assertThat(count).isEqualTo(10);

      connection.close();
    }
  }
}
