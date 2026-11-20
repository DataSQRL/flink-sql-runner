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
package sample.camel;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Every sample's DDL plans against its source class: the columns match the record. */
class SampleTablesTest {

  private static final String STATEMENT_END = ";";

  @ParameterizedTest
  @ValueSource(
      strings = {
        "accounts",
        "orders",
        "shipments",
        "ofac_sdn",
        "gl_postings",
        "recent_changes",
        "csv_drop"
      })
  void everySampleTablePlans(String table) throws IOException {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

    tEnv.executeSql(ddl(table));

    assertThat(tEnv.explainSql("SELECT * FROM " + table)).contains(table);
  }

  private static String ddl(String table) throws IOException {
    try (InputStream in =
        SampleTablesTest.class.getResourceAsStream("/sample/camel/" + table + ".sql")) {
      String ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8).strip();
      return ddl.endsWith(STATEMENT_END)
          ? ddl.substring(0, ddl.length() - STATEMENT_END.length())
          : ddl;
    }
  }
}
