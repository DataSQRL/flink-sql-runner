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
package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.io.Resources;
import java.io.File;
import java.net.URISyntaxException;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import picocli.CommandLine;

@ExtendWith(MiniClusterExtension.class)
class SqlRunnerTest {

  private File sqlFile;
  private File sqlUdfFile;
  private File planFile;
  private File configDir;
  private String udfPath;

  @BeforeEach
  void setUp() throws URISyntaxException {
    sqlFile = new File(Resources.getResource("sql/test_sql.sql").toURI());
    sqlUdfFile = new File(Resources.getResource("sql/test_udf_sql.sql").toURI());
    planFile = new File(Resources.getResource("plans/test_plan.json").toURI());
    configDir = new File(Resources.getResource("config").toURI());

    // Set UDF path to the 'udfs' directory in resources
    udfPath = new File(Resources.getResource("udfs").toURI()).getAbsolutePath();
  }

  @Test
  void testCommandLineInvocationWithSqlFile() {
    // Simulating passing command-line arguments using picocli
    String[] args = {"-s", sqlFile.getAbsolutePath()};

    // Use CommandLine to parse and execute
    var cmd = new CommandLine(new FlinkMain.SqlRunner());
    var exitCode = cmd.execute(args); // Executes the SqlRunner logic with arguments

    // Assert the exit code is as expected (0 for success)
    assertEquals(0, exitCode);
  }
}
