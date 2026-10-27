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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.awaitility.core.ThrowingRunnable;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

class KafkaSourceWatermarkIT extends AbstractITSupport {

  private static final String TOPIC = "source-watermark-it";
  private static final String IDLE_ADVANCE_TOPIC = "source-watermark-idle-advance-it";
  private static final String NO_IDLE_ADVANCE_TOPIC = "source-watermark-no-idle-advance-it";
  private static final String IDLE_ADVANCE_BACKLOG_TOPIC =
      "source-watermark-idle-advance-backlog-it";
  private static final String IDLE_ADVANCE_REPLAY_TOPIC = "source-watermark-idle-advance-replay-it";
  private static final String IDLE_ADVANCE_TRANSACTIONAL_TOPIC =
      "source-watermark-idle-advance-transactional-it";

  /** Records the replay test leaves to the rate-limited source, which reads one a second. */
  private static final int REPLAYED_RECORDS = 10;

  /** Records the transactional writers commit. */
  private static final int TRANSACTIONAL_RECORDS = 20;

  /** Records written in a transaction that is then aborted. */
  private static final int ABORTED_RECORDS = 3;

  /** How old the idle partition's record is when the job starts and reads it. */
  private static final Duration BACKLOG_AGE = Duration.ofSeconds(20);

  /**
   * Idle advancement closes a live 1 s window in about timeout + safety margin + window size, ~3 s.
   * A watermark that still carries the backlog's age needs at least {@link #BACKLOG_AGE} on top.
   */
  private static final Duration LIVE_WINDOW_DEADLINE = Duration.ofSeconds(10);

  @Test
  void givenKafkaSourceWatermark_whenWindowing_thenWatermarkClosesWindows() throws Exception {
    var resultDao = emptyResults();
    createTopic(TOPIC, false);
    produceTimestampedRecords("/it/sqlfile/kafka_source_watermark_produce.sql");

    runJob(
        "/it/sqlfile/kafka_source_watermark.sql",
        jobId ->
            untilAssert(
                () -> {
                  assertThat(resultDao.getRowCount()).isGreaterThan(0);
                  assertThat(resultDao.getTotalCount()).isGreaterThan(0);
                }));
  }

  @Test
  void givenSparseKafkaSourceWatermark_whenIdleAdvanceEnabled_thenWatermarkClosesWindows()
      throws Exception {
    var resultDao = emptyResults();
    createTopic(IDLE_ADVANCE_TOPIC, true);

    runJob(
        "/it/sqlfile/kafka_source_watermark_idle_advance.sql",
        jobId -> {
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
        });
  }

  /**
   * A fresh job reading history: one partition's newest record is already old when it is read, and
   * the partition is then idle. As the minimum across partitions it must not hold the source back
   * by that age, so a record on another partition closes its window on time.
   */
  @Test
  void givenIdlePartitionReadFromBacklog_whenIdleAdvanceEnabled_thenLiveWindowClosesOnTime()
      throws Exception {
    var resultDao = emptyResults();
    createTopic(IDLE_ADVANCE_BACKLOG_TOPIC, true, 2);
    produceJsonRecord(IDLE_ADVANCE_BACKLOG_TOPIC, 0, "{\"id\":0,\"payload\":\"backlog-record\"}");
    Thread.sleep(BACKLOG_AGE.toMillis());

    runJob(
        "/it/sqlfile/kafka_source_watermark_idle_advance_backlog.sql",
        jobId -> {
          // The backlog record's own window only needs the watermark to pass the record itself.
          untilAssert(() -> assertThat(resultDao.getRowCount()).isEqualTo(1));

          produceJsonRecord(
              IDLE_ADVANCE_BACKLOG_TOPIC, 1, "{\"id\":1,\"payload\":\"live-record\"}");

          untilAssert(
              LIVE_WINDOW_DEADLINE,
              () -> {
                assertThat(resultDao.getRowCount()).isEqualTo(2);
                assertThat(resultDao.getTotalCount()).isEqualTo(2);
              });
        });
  }

  /**
   * A partition still being replayed can be idle for longer than the idle-advance timeout, here
   * because the source is rate-limited to one record a second, while another partition has caught
   * up. Advancing the replaying partition would make its remaining records late, and the windows
   * they belong to would drop them.
   */
  @Test
  void givenPartitionStillReplaying_whenIdle_thenNoRecordArrivesLate() throws Exception {
    var resultDao = emptyResults();
    createTopic(IDLE_ADVANCE_REPLAY_TOPIC, true, 2);
    produceJsonRecord(
        IDLE_ADVANCE_REPLAY_TOPIC,
        0,
        IntStream.range(0, REPLAYED_RECORDS)
            .mapToObj(i -> "{\"id\":" + i + ",\"payload\":\"replayed-record\"}")
            .toArray(String[]::new));
    produceJsonRecord(IDLE_ADVANCE_REPLAY_TOPIC, 1, "{\"id\":-1,\"payload\":\"caught-up\"}");
    // Old enough that a watermark following the wall clock would be past all of them.
    Thread.sleep(5_000L);

    runJob(
        "/it/sqlfile/kafka_source_watermark_idle_advance_replay.sql",
        jobId ->
            untilAssert(
                Duration.ofSeconds(60),
                () -> assertThat(resultDao.getTotalCount()).isEqualTo(REPLAYED_RECORDS + 1)));
  }

  /**
   * Offsets that hold no record never reach a read_committed source: the commit markers of writers
   * with transactions of their own, such as the subtasks of an exactly-once sink, and the records
   * and marker of an aborted transaction. A partition whose log ends in any number of them must
   * still count as caught up, or the source would never close its final windows.
   */
  @Test
  void
      givenTransactionalTopic_whenIdleAdvanceEnabled_thenMarkersAndAbortedRecordsDoNotHoldBackWatermark()
          throws Exception {
    var resultDao = emptyResults();
    createTopic(IDLE_ADVANCE_TRANSACTIONAL_TOPIC, true, 1);
    produceTransactions(IDLE_ADVANCE_TRANSACTIONAL_TOPIC, TRANSACTIONAL_RECORDS, ABORTED_RECORDS);

    runJob(
        "/it/sqlfile/kafka_source_watermark_idle_advance_transactional.sql",
        jobId ->
            untilAssert(
                Duration.ofSeconds(30),
                () -> assertThat(resultDao.getTotalCount()).isEqualTo(TRANSACTIONAL_RECORDS)));
  }

  @Test
  void givenSparseKafkaSourceWatermark_whenIdleAdvanceDisabled_thenWatermarkDoesNotCloseWindow()
      throws Exception {
    var resultDao = emptyResults();
    createTopic(NO_IDLE_ADVANCE_TOPIC, true);

    runJob(
        "/it/sqlfile/kafka_source_watermark_no_idle_advance.sql",
        jobId -> {
          untilAssert(
              () ->
                  assertThat(client.getJobStatusInfo(jobId).getStatus())
                      .isEqualTo(JobStatus.RUNNING));

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
        });
  }

  /** Runs the job in {@code sqlFile}, checks it, and cancels it; failures carry Flink's logs. */
  private void runJob(String sqlFile, JobCheck check) throws Exception {
    String jobId = flinkRun("--sqlfile", sqlFile);
    try {
      check.run(jobId);
    } catch (Throwable t) {
      throw new AssertionError(readFlinkLogs(), t);
    } finally {
      cancelJob(jobId);
    }
  }

  private void untilAssert(Duration atMost, ThrowingRunnable assertion) {
    await().atMost(atMost).pollInterval(500, MILLISECONDS).untilAsserted(assertion);
  }

  /** The results table the jobs write to, created if need be and emptied. */
  private ResultDao emptyResults() {
    var resultDao = connect();
    resultDao.createTable();
    resultDao.truncateTable();
    return resultDao;
  }

  private String readFlinkLogs() throws Exception {
    Container.ExecResult result =
        flinkContainer.execInContainer(
            "bash", "-c", "for f in /opt/flink/log/*; do echo ==== $f ====; tail -200 $f; done");
    return result.getStdout() + result.getStderr();
  }

  private void createTopic(String topic, boolean logAppendTime) throws Exception {
    createTopic(topic, logAppendTime, 1);
  }

  private void createTopic(String topic, boolean logAppendTime, int partitions) throws Exception {
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
                String.valueOf(partitions)));
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

  /**
   * Runs {@code kafka/TransactionalProducer.java} in the Flink container, which reaches the broker
   * and has the Kafka client on its classpath.
   */
  private void produceTransactions(String topic, int committed, int aborted) throws Exception {
    flinkContainer.copyFileToContainer(
        MountableFile.forClasspathResource("kafka/TransactionalProducer.java"),
        "/tmp/TransactionalProducer.java");

    Container.ExecResult result =
        flinkContainer.execInContainer(
            "java",
            "-cp",
            "/opt/flink/lib/*",
            "/tmp/TransactionalProducer.java",
            "redpanda:9092",
            topic,
            String.valueOf(committed),
            String.valueOf(aborted));

    assertThat(result.getExitCode())
        .withFailMessage(result.getStdout() + result.getStderr())
        .isZero();
  }

  private void produceJsonRecord(String topic, String json) throws Exception {
    produceJsonRecord(topic, -1, json);
  }

  /**
   * Produces one record per JSON document to {@code partition}, or lets the partitioner choose when
   * it is negative.
   */
  private void produceJsonRecord(String topic, int partition, String... jsons) throws Exception {
    var quoted = new StringBuilder();
    for (String json : jsons) {
      quoted.append(" '").append(json).append('\'');
    }

    Container.ExecResult result =
        redpandaContainer.execInContainer(
            "bash",
            "-c",
            String.format(
                "printf '%%s\\n'%s | rpk topic produce %s --brokers redpanda:9092 --partition %d",
                quoted, topic, partition));

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

  @FunctionalInterface
  private interface JobCheck {

    void run(String jobId) throws Throwable;
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
