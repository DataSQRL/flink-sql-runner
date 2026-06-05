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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nextbreakpoint.flink.client.model.JobStatus;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import java.util.ArrayList;
import java.util.List;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

class KafkaSourceWatermarkIT extends AbstractITSupport {

  private static final String TOPIC = "source-watermark-it";
  private static final String IDLE_ADVANCE_TOPIC = "source-watermark-idle-advance-it";
  private static final String NO_IDLE_ADVANCE_TOPIC = "source-watermark-no-idle-advance-it";

  @Test
  void givenKafkaSourceWatermark_whenWindowing_thenWatermarkClosesWindows() throws Exception {
    var resultDao = connect();
    resultDao.createTable();
    resultDao.truncateTable();

    createTopic(TOPIC, false);
    produceTimestampedRecords("/it/sqlfile/kafka_source_watermark_produce.sql");

    String jobId = flinkRun("--sqlfile", "/it/sqlfile/kafka_source_watermark.sql");

    try {
      untilAssert(
          () -> {
            assertThat(resultDao.getRowCount()).isGreaterThan(0);
            assertThat(resultDao.getTotalCount()).isGreaterThan(0);
          });
    } catch (Throwable t) {
      throw new AssertionError(readFlinkLogs(), t);
    } finally {
      cancelJob(jobId);
    }
  }

  @Test
  void givenSparseKafkaSourceWatermark_whenIdleAdvanceEnabled_thenWatermarkClosesWindows()
      throws Exception {
    var resultDao = connect();
    resultDao.createTable();
    resultDao.truncateTable();

    createTopic(IDLE_ADVANCE_TOPIC, true);
    String jobId = flinkRun("--sqlfile", "/it/sqlfile/kafka_source_watermark_idle_advance.sql");

    try {
      produceJsonRecord(IDLE_ADVANCE_TOPIC, "{\"id\":0,\"payload\":\"idle-record-0\"}");

      untilAssert(
          () -> {
            assertThat(resultDao.getRowCount()).isEqualTo(1);
            assertThat(resultDao.getTotalCount()).isEqualTo(1);
          });

      produceJsonRecord(IDLE_ADVANCE_TOPIC, "{\"id\":1,\"payload\":\"idle-record-1\"}");

      untilAssert(
          () -> {
            assertThat(resultDao.getRowCount()).isEqualTo(2);
            assertThat(resultDao.getTotalCount()).isEqualTo(2);
          });
    } catch (Throwable t) {
      throw new AssertionError(readFlinkLogs(), t);
    } finally {
      cancelJob(jobId);
    }
  }

  @Test
  void givenSparseKafkaSourceWatermark_whenIdleAdvanceDisabled_thenWatermarkDoesNotCloseWindow()
      throws Exception {
    var resultDao = connect();
    resultDao.createTable();
    resultDao.truncateTable();

    createTopic(NO_IDLE_ADVANCE_TOPIC, true);
    String jobId = flinkRun("--sqlfile", "/it/sqlfile/kafka_source_watermark_no_idle_advance.sql");

    try {
      untilAssert(
          () ->
              assertThat(client.getJobStatusInfo(jobId).getStatus()).isEqualTo(JobStatus.RUNNING));

      produceJsonRecord(NO_IDLE_ADVANCE_TOPIC, "{\"id\":0,\"payload\":\"idle-record-0\"}");

      await()
          .during(5, SECONDS)
          .atMost(6, SECONDS)
          .pollInterval(500, MILLISECONDS)
          .untilAsserted(
              () -> {
                assertThat(resultDao.getRowCount()).isZero();
                assertThat(resultDao.getTotalCount()).isZero();
              });
    } catch (Throwable t) {
      throw new AssertionError(readFlinkLogs(), t);
    } finally {
      cancelJob(jobId);
    }
  }

  private String readFlinkLogs() throws Exception {
    Container.ExecResult result =
        flinkContainer.execInContainer(
            "bash", "-c", "for f in /opt/flink/log/*; do echo ==== $f ====; tail -200 $f; done");
    return result.getStdout() + result.getStderr();
  }

  private void createTopic(String topic, boolean logAppendTime) throws Exception {
    var command =
        new ArrayList<>(
            List.of(
                "rpk",
                "topic",
                "create",
                topic,
                "--brokers",
                "redpanda:9092",
                "--partitions",
                "1"));
    if (logAppendTime) {
      command.add("--topic-config");
      command.add("message.timestamp.type=LogAppendTime");
    }

    Container.ExecResult result = redpandaContainer.execInContainer(command.toArray(new String[0]));

    assertThat(result.getExitCode())
        .withFailMessage(result.getStdout() + result.getStderr())
        .isZero();
  }

  private void produceTimestampedRecords(String sqlFile) throws Exception {
    String jobId = flinkRun("--sqlfile", sqlFile);

    untilAssert(
        () -> assertThat(client.getJobStatusInfo(jobId).getStatus()).isEqualTo(JobStatus.FINISHED));
  }

  private void produceJsonRecord(String topic, String json) throws Exception {
    Container.ExecResult result =
        redpandaContainer.execInContainer(
            "bash",
            "-c",
            String.format(
                "printf '%%s\\n' '%s' | rpk topic produce %s --brokers redpanda:9092",
                json, topic));

    assertThat(result.getExitCode())
        .withFailMessage(result.getStdout() + result.getStderr())
        .isZero();
  }

  private void cancelJob(String jobId) {
    try {
      client.cancelJob(jobId, TerminationMode.CANCEL);
    } catch (Exception ignored) {
    }
  }

  private ResultDao connect() {
    var mappedPort = postgresContainer.getMappedPort(5432);
    var jdbi =
        Jdbi.create(
            "jdbc:postgresql://localhost:" + mappedPort + "/datasqrl", "postgres", "postgres");
    jdbi.installPlugin(new SqlObjectPlugin());
    return jdbi.onDemand(ResultDao.class);
  }

  interface ResultDao {

    @SqlUpdate(
        "CREATE TABLE IF NOT EXISTS source_watermark_results ("
            + "window_start TIMESTAMP NOT NULL, "
            + "window_end TIMESTAMP NOT NULL, "
            + "cnt BIGINT NOT NULL)")
    void createTable();

    @SqlUpdate("TRUNCATE source_watermark_results")
    void truncateTable();

    @SqlQuery("SELECT count(*) FROM source_watermark_results")
    long getRowCount();

    @SqlQuery("SELECT coalesce(sum(cnt), 0) FROM source_watermark_results")
    long getTotalCount();
  }
}
