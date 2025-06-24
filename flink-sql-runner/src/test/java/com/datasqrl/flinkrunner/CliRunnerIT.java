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
package com.datasqrl.flinkrunner;

import java.util.ArrayList;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CliRunnerIT extends AbstractITSupport {

  static Stream<Arguments> execArgs() {
    return Stream.of(
        Arguments.of("sqlfile", "flink.sql", true),
        Arguments.of("sqlfile", "flink.sql", false),
        Arguments.of("sqlfile", "test_sql.sql", true),
        Arguments.of("sqlfile", "test_sql.sql", false),
        Arguments.of("planfile", "compiled_plan.json", true),
        Arguments.of("planfile", "compiled_plan.json", false),
        Arguments.of("planfile", "test_plan.json", true),
        Arguments.of("planfile", "test_plan.json", false));
  }

  @ParameterizedTest
  @MethodSource("execArgs")
  void givenSqlOrPlan_whenExecuting_thenSuccess(String option, String file, boolean config)
      throws Exception {
    var args = new ArrayList<String>();
    args.add("--" + option);
    args.add("/it/" + option + "/" + file);
    if (config) {
      args.add("--config-dir");
      args.add("/it/config/");
    }

    String jobId = flinkRun(args);
    assertJobIsRunning(jobId);
  }

  static Stream<Arguments> udfExecArgs() {
    return Stream.of(
        Arguments.of("sqlfile", "test_udf_sql.sql"),
        Arguments.of("planfile", "compiled_plan_udf.json"));
  }

  @ParameterizedTest
  @MethodSource("udfExecArgs")
  void givenUdfSqlScript_whenExecuting_thenSuccess(String option, String file) throws Exception {
    var filePath = "/it/" + option + "/" + file;
    String jobId = flinkRun("--" + option, filePath, "--udfpath", "/it/udfs/");
    assertJobIsRunning(jobId);
  }

  @Test
  void givenKafkaPlanScript_whenExecuting_thenSuccess() throws Exception {
    String jobId = flinkRun("--planfile", "/it/planfile/kafka_plan.json");
    assertJobIsRunning(jobId);
  }
}
