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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.MoreCollectors;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;

@Slf4j
public class SavepointIT extends AbstractITSupport {

  @TempDir private Path tempDir;

  @Test
  void givenPlanScript_whenSavepoint_thenResumeFromSavepointSuccessfully() throws Exception {
    var transactionDao = connect();

    // clear any old data
    transactionDao.truncateTable();
    assertThat(transactionDao.getRowCount()).isZero();

    // change the compilation plan
    updateCompiledPlan(3);

    var args = new ArrayList<String>();
    var planFile = "/it/sqrl/compiled-plan.json";
    args.add("--planfile");
    args.add(planFile);
    args.add("--config-dir");
    args.add("/it/config/");

    var jobId = flinkRun(args);
    assertJobIsRunning(jobId);

    untilAssert(() -> assertThat(transactionDao.getRowCount()).isEqualTo(10));
    // test if initial change to compilation plan took effect
    assertThat(transactionDao.getDoubleTA()).isEqualTo(3);

    Container.ExecResult cmdRes =
        flinkContainer.execInContainer(
            "flink", "stop", jobId, "--savepointPath", CONTAINER_TEST_OUT_PATH);

    log.debug("Flink container STDOUT: {}", cmdRes.getStdout());
    log.debug("Flink container STDERR: {}", cmdRes.getStderr());

    // check if STOPed, make sure no new records are create
    untilAssert(
        () ->
            assertThat(
                    transactionDao.getRowCount(
                        Timestamp.valueOf(
                            ZonedDateTime.now()
                                .withZoneSameInstant(ZoneId.of("Z"))
                                .toLocalDateTime())))
                .isZero());

    copyDirectoryFromContainer(flinkContainer, CONTAINER_TEST_OUT_PATH, tempDir);
    assertThat(tempDir).isNotEmptyDirectory();

    Path savePointFile;
    try (var filePathStream = Files.walk(tempDir)) {
      savePointFile =
          filePathStream
              .filter(Files::isRegularFile)
              .filter(path -> "_metadata".equals(path.getFileName().toString()))
              .collect(MoreCollectors.onlyElement());
    }

    // make sure savepoint was created
    assertThat(savePointFile).exists();

    // change compiled plan again
    updateCompiledPlan(5);

    var restoration =
        Timestamp.valueOf(
            ZonedDateTime.now().withZoneSameInstant(ZoneId.of("Z")).toLocalDateTime());

    var savepointDirName = savePointFile.getParent().getFileName().toString();
    // restart with savepoint
    jobId = flinkRun(args, CONTAINER_TEST_OUT_PATH + '/' + savepointDirName);
    assertJobIsRunning(jobId);

    untilAssert(() -> assertThat(transactionDao.getRowCount(restoration)).isEqualTo(10));
    assertThat(transactionDao.getDoubleTA()).isEqualTo(5);
  }

  private TransactionDao connect() {
    var mappedPort = postgresContainer.getMappedPort(5432);
    var jdbi =
        Jdbi.create(
            "jdbc:postgresql://localhost:" + mappedPort + "/datasqrl", "postgres", "postgres");
    jdbi.installPlugin(new SqlObjectPlugin());
    jdbi.setSqlLogger(
        new SqlLogger() {
          @Override
          public void logAfterExecution(StatementContext ctx) {
            log.info(
                "Executed '{}' with parameters '{}'",
                ctx.getParsedSql().getSql(),
                ctx.getBinding());
          }

          @Override
          public void logException(StatementContext ctx, SQLException ex) {
            log.info(
                "Exception while executing '{}' with parameters '{}'",
                ctx.getParsedSql().getSql(),
                ctx.getBinding());
          }
        });

    return jdbi.onDemand(TransactionDao.class);
  }

  @SneakyThrows
  private void updateCompiledPlan(int newValue) {
    var contents = Files.readString(Path.of("src/test/resources/sqrl/compiled-plan.json"), UTF_8);
    contents = contents.replace("\"value\" : 2", "\"value\" : " + newValue);
    Files.writeString(Path.of("target/test-classes/sqrl/compiled-plan.json"), contents, UTF_8);
  }

  interface TransactionDao {

    @SqlQuery(
        "SELECT count(1) FROM public.transactionbymerchant_1 t where t.\"__timestamp\" > :minDate")
    int getRowCount(@Bind("minDate") Timestamp minDate);

    @SqlQuery("SELECT count(1) FROM public.transactionbymerchant_1 t")
    int getRowCount();

    @SqlQuery("SELECT double_ta FROM public.transactionbymerchant_1 limit 1")
    int getDoubleTA();

    @SqlQuery(
        "SELECT max(transactionbymerchant_1.\"__timestamp\") FROM public.transactionbymerchant_1")
    OffsetDateTime getMaxTimestamp();

    @SqlUpdate("TRUNCATE public.transactionbymerchant_1")
    void truncateTable();
  }
}
