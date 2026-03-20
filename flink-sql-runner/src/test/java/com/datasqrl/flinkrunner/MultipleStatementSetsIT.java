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
package com.datasqrl.flinkrunner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class MultipleStatementSetsIT extends AbstractITSupport {

  @Test
  void given_multipleStatementSets_when_batchExecution_then_allStatementSetsComplete()
      throws Exception {
    var execCmd =
        new ArrayList<>(
            List.of(
                "flink",
                "run",
                "./plugins/flink-sql-runner/flink-sql-runner.uber.jar",
                "--sqlfile",
                "/it/sqlfile/test_multiple_statement_sets.sql",
                "--mode",
                "BATCH"));

    var execRes = flinkContainer.execInContainer(execCmd.toArray(new String[0]));
    var stdOut = execRes.getStdout();
    var stdErr = execRes.getStderr();

    assertThat(stdErr)
        .withFailMessage("Execution failed with: %s", stdErr)
        .doesNotContain("The program finished with the following exception:");

    var jobIds =
        stdOut
            .lines()
            .filter(line -> line.startsWith("Job has been submitted with JobID"))
            .map(line -> line.substring(line.lastIndexOf(" ") + 1).trim())
            .toList();

    assertThat(jobIds).as("Expected two jobs submitted (one per EXECUTE STATEMENT SET)").hasSize(2);

    var tmLogs = flinkContainer.execInContainer("bash", "-c", "cat /opt/flink/log/*taskexecutor*");

    assertThat(tmLogs.getStdout())
        .as("First statement set output (PrintOutputA) should be present")
        .contains("PrintOutputA");

    assertThat(tmLogs.getStdout())
        .as("Second statement set output (PrintOutputB) should be present")
        .contains("PrintOutputB");
  }
}
