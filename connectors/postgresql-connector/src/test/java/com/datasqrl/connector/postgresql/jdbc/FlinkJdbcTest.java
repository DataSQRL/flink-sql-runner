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
package com.datasqrl.connector.postgresql.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.PostgreSQLContainer;

@ExtendWith(MiniClusterExtension.class)
public class FlinkJdbcTest {

  @Test
  public void testWriteAndReadToPostgres() throws Exception {
    try (PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:14")) {
      postgresContainer.start();
      try (Connection conn =
              DriverManager.getConnection(
                  postgresContainer.getJdbcUrl(),
                  postgresContainer.getUsername(),
                  postgresContainer.getPassword());
          Statement stmt = conn.createStatement()) {
        String createTableSQL = "CREATE TABLE test_table (" + "    id BIGINT, name VARCHAR " + ")";
        stmt.executeUpdate(createTableSQL);
      }

      // Set up Flink mini cluster environment
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

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
      Connection connection = postgresContainer.createConnection("");
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM test_table");

      int count = 0;
      if (resultSet.next()) {
        count = resultSet.getInt(1);
      }

      // Validate that 10 rows were inserted
      assertEquals(10, count);

      connection.close();
    }
  }
}
