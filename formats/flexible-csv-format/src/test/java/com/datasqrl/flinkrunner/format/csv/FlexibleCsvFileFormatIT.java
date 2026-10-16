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
package com.datasqrl.flinkrunner.format.csv;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Integration tests for reading {@code flexible-csv} files through the filesystem connector. */
@ExtendWith(MiniClusterExtension.class)
class FlexibleCsvFileFormatIT {

  @TempDir Path tempDir;

  private TableEnvironment tEnv;

  @BeforeEach
  void beforeEach() {
    tEnv = TableEnvironmentImpl.create(EnvironmentSettings.newInstance().inBatchMode().build());
  }

  @Test
  void givenPlainAndCompressedFilesWithHeaders_whenSelect_thenReturnsDataRowsOfEveryFile()
      throws IOException {
    Files.writeString(tempDir.resolve("part-1.csv"), "id,name\n1,a\n2,b\n");
    try (var out = new GZIPOutputStream(Files.newOutputStream(tempDir.resolve("part-2.csv.gz")))) {
      out.write("id,name\n3,c\n4,d\n".getBytes(StandardCharsets.UTF_8));
    }
    createTable("");

    assertThat(query("SELECT id, name FROM csv_source"))
        .containsExactlyInAnyOrder(Row.of(1, "a"), Row.of(2, "b"), Row.of(3, "c"), Row.of(4, "d"));
  }

  @Test
  void givenProjectedColumn_whenSelect_thenSkipsHeader() throws IOException {
    Files.writeString(tempDir.resolve("data.csv"), "id,name\n1,a\n2,b\n");
    createTable("");

    assertThat(query("SELECT name FROM csv_source"))
        .containsExactlyInAnyOrder(Row.of("a"), Row.of("b"));
  }

  @Test
  void givenIgnoreParseErrors_whenSelect_thenSetsUnparsableFieldsToNull() throws IOException {
    Files.writeString(tempDir.resolve("data.csv"), "id,name\n1,a\nx,b\n3,c\n");
    createTable(", 'flexible-csv.ignore-parse-errors' = 'true'");

    assertThat(query("SELECT id, name FROM csv_source"))
        .containsExactlyInAnyOrder(Row.of(1, "a"), Row.of(null, "b"), Row.of(3, "c"));
  }

  private void createTable(String extraOptions) {
    tEnv.executeSql(
        """
            CREATE TABLE csv_source (
              id INT,
              name STRING
            ) WITH (
              'connector' = 'filesystem',
              'path' = '%s',
              'format' = 'flexible-csv'%s
            )"""
            .formatted(tempDir.toUri(), extraOptions));
  }

  private List<Row> query(String sql) {
    var rows = new ArrayList<Row>();
    try (var result = tEnv.executeSql(sql).collect()) {
      result.forEachRemaining(rows::add);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return rows;
  }
}
