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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody.StatusEnum;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.flink.shaded.curator5.com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkMainIT extends AbstractITSupport {

  static Stream<Arguments> sqlScripts() {
    var scripts = List.of("flink.sql", "test_sql.sql");
    var config = List.of(true, false);
    var block = List.of(true, false);
    return Lists.cartesianProduct(scripts, config, block).stream()
        .map(pair -> Arguments.of(pair.toArray()));
  }

  @ParameterizedTest(name = "{0} {1} {2}")
  @MethodSource("sqlScripts")
  void givenSqlScript_whenExecuting_thenSuccess(String filename, boolean config, boolean block) {
    assumeThat(block).as("`--block` doesn't seem to work").isFalse();

    String sqlFile = "/opt/flink/usrlib/sql/" + filename;
    var args = new ArrayList<String>();
    args.add("--sqlfile");
    args.add(sqlFile);
    if (config) {
      args.add("--config-dir");
      args.add("/opt/flink/usrlib/config/");
    }
    if (block) {
      args.add("--block");
    }
    execute(args.toArray(String[]::new));
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({"compiled-plan.json", "test_plan.json"})
  void givenPlansScript_whenExecuting_thenSuccess(String filename) {
    String planFile = "/opt/flink/usrlib/plans/" + filename;
    execute("--planfile", planFile);
  }

  @SneakyThrows
  void execute(String... arguments) {
    File jarFile = new File("target/flink-jar-runner-1.0.0-SNAPSHOT.jar");

    JarUploadResponseBody uploadResponse = client.uploadJar(jarFile);

    assertThat(uploadResponse.getStatus()).isEqualTo(StatusEnum.SUCCESS);

    // Step 2: Extract jarId from the response
    String jarId =
        uploadResponse.getFilename().substring(uploadResponse.getFilename().lastIndexOf("/") + 1);

    // Step 3: Submit the job
    assertThatNoException()
        .as("Running script %s", Arrays.toString(arguments))
        .isThrownBy(
            () -> {
              JarRunResponseBody jobResponse =
                  client.runJar(
                      jarId,
                      null,
                      null,
                      null,
                      Arrays.stream(arguments).collect(Collectors.joining(",")),
                      null,
                      1);
              String jobId = jobResponse.getJobid();
              assertThat(jobId).isNotNull();
            });
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({"test_udf_sql.sql"})
  void givenUdfSqlScript_whenExecuting_thenSuccess(String filename) {
    String sqlFile = "/opt/flink/usrlib/sql/" + filename;
    execute("--sqlfile", sqlFile, "--udfpath", "/opt/flink/usrlib/udfs/");
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({"compiled-plan-udf.json"})
  void givenUdfPlansScript_whenExecuting_thenSuccess(String filename) {
    String planFile = "/opt/flink/usrlib/plans/" + filename;
    execute("--planfile", planFile, "--udfpath", "/opt/flink/usrlib/udfs/");
  }
}
