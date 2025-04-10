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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.MoreCollectors;
import com.nextbreakpoint.flink.client.model.JarRunResponseBody;
import com.nextbreakpoint.flink.client.model.JobExceptionsInfoWithHistory;
import com.nextbreakpoint.flink.client.model.JobStatus;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import com.nextbreakpoint.flink.client.model.UploadStatus;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.flink.shaded.curator5.com.google.common.base.Objects;
import org.apache.flink.shaded.curator5.com.google.common.collect.Lists;
import org.awaitility.core.ThrowingRunnable;
import org.jacoco.core.instr.Instrumenter;
import org.jacoco.core.runtime.OfflineInstrumentationAccessGenerator;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FlinkMainIT extends AbstractITSupport {

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

  @BeforeEach
  @AfterEach
  void terminateJobs() throws Exception {
    var jobs = client.getJobsOverview();
    jobs.getJobs().stream()
        .filter(job -> Objects.equal(job.getState(), JobStatus.RUNNING))
        .forEach(job -> stopJobs(job.getJid()));
  }

  static Stream<Arguments> sqlScripts() {
    var scripts = List.of("flink.sql", "test_sql.sql");
    var config = List.of(true, false);
    return Lists.cartesianProduct(scripts, config).stream()
        .map(pair -> Arguments.of(pair.toArray()));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("sqlScripts")
  void givenSqlScript_whenExecuting_thenSuccess(String filename, boolean config) {
    String sqlFile = "/opt/flink/usrlib/sql/" + filename;
    var args = new ArrayList<String>();
    args.add("--sqlfile");
    args.add(sqlFile);
    if (config) {
      args.add("--config-dir");
      args.add("/opt/flink/usrlib/config/");
    }
    execute(args.toArray(String[]::new));
  }

  static Stream<Arguments> planScripts() {
    var scripts = List.of("compiled-plan.json", "test_plan.json");
    var config = List.of(true, false);
    return Lists.cartesianProduct(scripts, config).stream()
        .map(pair -> Arguments.of(pair.toArray()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("planScripts")
  void givenPlansScript_whenExecuting_thenSuccess(String filename, boolean config) {
    String planFile = "/opt/flink/usrlib/plans/" + filename;
    var args = new ArrayList<String>();
    args.add("--planfile");
    args.add(planFile);
    if (config) {
      args.add("--config-dir");
      args.add("/opt/flink/usrlib/config/");
    }
    execute(args.toArray(String[]::new));
  }

  @Test
  void givenPlanScript_whenCheckPoint_thenResumeSuccessfulySuccess() throws Exception {
    var transactionDao = connect();
    // clear any old data
    transactionDao.truncateTable();
    assertThat(transactionDao.getRowCount()).isZero();

    // change the compilation plan
    updateCompiledPlan(3);

    String planFile = "/opt/flink/usrlib/sqrl/compiled-plan.json";
    var args = new ArrayList<String>();
    args.add("--planfile");
    args.add(planFile);
    args.add("--config-dir");
    args.add("/opt/flink/usrlib/config/");
    var jobResponse = execute(args.toArray(String[]::new));

    untilAssert(() -> assertThat(transactionDao.getRowCount()).isEqualTo(10));
    // test if initial change to compilation plan took effect
    assertThat(transactionDao.getDoubleTA()).isEqualTo(3);

    Path savePoint = Path.of("/tmp/flink-sql-runner/" + System.currentTimeMillis());

    // take a savepoint
    CommandLineUtil.execute(
        Path.of("."),
        "docker exec -t flink-sql-runner-jobmanager-1 bin/flink stop "
            + jobResponse.getJobid()
            + " --savepointPath "
            + savePoint);

    // check if STOPed, make sure no new records are create
    untilAssert(
        () ->
            assertThat(
                    transactionDao.getRowCount(
                        Timestamp.valueOf(
                            ZonedDateTime.now()
                                .withZoneSameInstant(ZoneId.of("Z"))
                                .toLocalDateTime())))
                .isEqualTo(0));
    assertThat(savePoint).isNotEmptyDirectory();

    Path savePointFile;
    try (var filePathStream = Files.walk(savePoint)) {
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

    // restart with savepoint
    restoreAndExecute(savePointFile.getParent().toString(), args.toArray(String[]::new));

    untilAssert(() -> assertThat(transactionDao.getRowCount(restoration)).isEqualTo(10));
    assertThat(transactionDao.getDoubleTA()).isEqualTo(5);
  }

  @SneakyThrows
  private void updateCompiledPlan(int newValue) {
    var contents = Files.readString(Path.of("src/test/resources/sqrl/compiled-plan.json"), UTF_8);
    contents = contents.replace("\"value\" : 2", "\"value\" : " + newValue);
    Files.writeString(Path.of("target/test-classes/sqrl/compiled-plan.json"), contents, UTF_8);
  }

  public void untilAssert(ThrowingRunnable assertion) {
    await()
        .atMost(20, SECONDS)
        .pollInterval(100, MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(assertion);
  }

  private TransactionDao connect() {
    var jdbi = Jdbi.create("jdbc:postgresql://localhost:5432/datasqrl", "postgres", "postgres");
    jdbi.installPlugin(new SqlObjectPlugin());
    jdbi.setSqlLogger(
        new SqlLogger() {
          @Override
          public void logAfterExecution(StatementContext context) {
            System.out.printf(
                "Executed in '%s' with parameters '%s'\n",
                context.getParsedSql().getSql(), context.getBinding());
          }

          @Override
          public void logException(StatementContext context, SQLException ex) {
            System.out.printf(
                "Exception while executing '%s' with parameters '%s'\n",
                context.getParsedSql().getSql(), context.getBinding(), ex);
          }
        });

    return jdbi.onDemand(TransactionDao.class);
  }

  JarRunResponseBody execute(String... arguments) {
    return restoreAndExecute(null, arguments);
  }

  private int instrument(final File src, final File dest) throws IOException {
    if (dest.exists()) {
      return 0;
    }
    var instrumenter = new Instrumenter(new OfflineInstrumentationAccessGenerator());

    dest.getParentFile().mkdirs();
    final InputStream input = new FileInputStream(src);
    try {
      final OutputStream output = new FileOutputStream(dest);
      try {
        return instrumenter.instrumentAll(input, output, src.getAbsolutePath());
      } finally {
        output.close();
      }
    } catch (final IOException e) {
      dest.delete();
      throw e;
    } finally {
      input.close();
    }
  }

  @SneakyThrows
  JarRunResponseBody restoreAndExecute(String savepointPath, String... arguments) {
    var originalFile = new File("target/flink-sql-runner.uber.jar");
    var instrumentedJarFile = new File("target/flink-sql-runner.jacoco.jar");
    instrument(originalFile, instrumentedJarFile);

    var uploadResponse = client.uploadJar(instrumentedJarFile);

    assertThat(uploadResponse.getStatus()).isEqualTo(UploadStatus.SUCCESS);

    // Step 2: Extract jarId from the response
    var jarId =
        uploadResponse.getFilename().substring(uploadResponse.getFilename().lastIndexOf("/") + 1);

    // Step 3: Submit the job
    var jobResponse =
        client.submitJobFromJar(
            jarId,
            null,
            savepointPath == null ? null : false,
            savepointPath,
            null,
            Arrays.stream(arguments).collect(Collectors.joining(",")),
            null,
            1);
    String jobId = jobResponse.getJobid();
    assertThat(jobId).isNotNull();

    SECONDS.sleep(2);

    return jobResponse;
  }

  @SneakyThrows
  private void stopJobs(String jobId) {
    var status = client.getJobStatusInfo(jobId);
    if (Objects.equal(status.getStatus(), JobStatus.RUNNING)) {
      client.cancelJob(jobId, TerminationMode.CANCEL);
    } else {
      JobExceptionsInfoWithHistory exceptions = client.getJobExceptions(jobId, 5, null);
      fail(exceptions.toString());
    }
  }

  @Test
  void givenUdfSqlScript_whenExecuting_thenSuccess() {
    String sqlFile = "/opt/flink/usrlib/sql/test_udf_sql.sql";
    execute("--sqlfile", sqlFile, "--udfpath", "/opt/flink/usrlib/udfs/");
  }

  @Test
  void givenUdfPlansScript_whenExecuting_thenSuccess() {
    String planFile = "/opt/flink/usrlib/plans/compiled-plan-udf.json";
    execute("--planfile", planFile, "--udfpath", "/opt/flink/usrlib/udfs/");
  }
}
