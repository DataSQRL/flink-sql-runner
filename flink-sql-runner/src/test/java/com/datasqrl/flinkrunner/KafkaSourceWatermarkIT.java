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

import com.nextbreakpoint.flink.client.model.JobStatus;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

class KafkaSourceWatermarkIT extends AbstractITSupport {

  private static final String TOPIC = "source-watermark-it";

  @Test
  void givenKafkaSourceWatermark_whenWindowing_thenWatermarkClosesWindows() throws Exception {
    var resultDao = connect();
    resultDao.createTable();
    resultDao.truncateTable();

    createTopic();
    produceTimestampedRecords();

    flinkRun("--sqlfile", "/it/sqlfile/kafka_source_watermark.sql");

    try {
      untilAssert(
          () -> {
            assertThat(resultDao.getRowCount()).isGreaterThan(0);
            assertThat(resultDao.getTotalCount()).isGreaterThan(0);
          });
    } catch (Throwable t) {
      throw new AssertionError(readFlinkLogs(), t);
    }
  }

  private String readFlinkLogs() throws Exception {
    Container.ExecResult result =
        flinkContainer.execInContainer(
            "bash", "-c", "for f in /opt/flink/log/*; do echo ==== $f ====; tail -200 $f; done");
    return result.getStdout() + result.getStderr();
  }

  private void createTopic() throws Exception {
    Container.ExecResult result =
        redpandaContainer.execInContainer(
            "rpk", "topic", "create", TOPIC, "--brokers", "redpanda:9092", "--partitions", "1");

    assertThat(result.getExitCode())
        .withFailMessage(result.getStdout() + result.getStderr())
        .isZero();
  }

  private void produceTimestampedRecords() throws Exception {
    String jobId = flinkRun("--sqlfile", "/it/sqlfile/kafka_source_watermark_produce.sql");

    untilAssert(
        () -> assertThat(client.getJobStatusInfo(jobId).getStatus()).isEqualTo(JobStatus.FINISHED));
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
