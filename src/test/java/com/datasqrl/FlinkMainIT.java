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

import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody.StatusEnum;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkMainIT extends AbstractITSupport {

  @ParameterizedTest(name = "{0}")
  @CsvSource({"flink.sql", "test_sql.sql"})
  void givenSqlScript_whenExecuting_thenSuccess(String filename) throws IOException, Exception {
    String sqlFile = "/opt/flink/usrlib/sql/" + filename;
    execute(filename, "--sqlfile", sqlFile);
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({"compiled-plan.json", "test_plan.json"})
  void givenPlansScript_whenExecuting_thenSuccess(String filename) throws IOException, Exception {
    String planFile = "/opt/flink/usrlib/plans/" + filename;
    execute(filename, "--planfile", planFile);
  }

  void execute(String filename, String... arguments) throws ApiException {
    File jarFile = new File("target/flink-jar-runner-1.0.0-SNAPSHOT.jar");

    JarUploadResponseBody uploadResponse = client.uploadJar(jarFile);

    assertThat(uploadResponse.getStatus()).isEqualTo(StatusEnum.SUCCESS);

    // Step 2: Extract jarId from the response
    String jarId =
        uploadResponse.getFilename().substring(uploadResponse.getFilename().lastIndexOf("/") + 1);

    // Step 3: Submit the job
    assertThatNoException()
        .as("Running script %s", filename)
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
  void givenUdfScript_whenExecuting_thenSuccess(String filename) throws IOException, Exception {
    String sqlFile = "/opt/flink/usrlib/sql/" + filename;
    execute(filename, "--sqlfile", sqlFile, "--udfpath", "/opt/flink/usrlib/udfs/");
  }
}
