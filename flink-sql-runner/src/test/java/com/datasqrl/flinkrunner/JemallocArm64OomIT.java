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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.awaitility.Awaitility.await;

import com.github.dockerjava.api.command.InspectContainerResponse.ContainerState;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

/**
 * Reproduces arm64 TaskManagers being OOMKilled: the image's jemalloc uses 64 KiB pages on a 4 KiB
 * page kernel and keeps ~400 MiB more than on amd64 after warm-up, exceeding the steady-state
 * limit.
 */
@Slf4j
class JemallocArm64OomIT {

  private static final DockerImageName IMAGE = DockerImageName.parse("flink-sql-runner");
  private static final long WARMUP_MEMORY_BYTES = 6000L * 1024 * 1024;
  private static final long STEADY_STATE_MEMORY_BYTES = 3800L * 1024 * 1024;
  private static final String TM_CPUSET = "0-3";
  private static final Duration WARMUP = Duration.ofMinutes(3);
  private static final Duration SURVIVAL_WINDOW = Duration.ofMinutes(3);
  private static final String SQL_PATH = "/opt/flink/usrlib/oom-repro.sql";
  private static final Pattern TASK_COUNTS =
      Pattern.compile("\"tasks\":\\{\"running\":(\\d+).*?\"total\":(\\d+)");

  private static final int STATEFUL_OPERATORS = 70;
  private static final int ROWS_PER_SECOND = 50_000;
  private static final int KEYS = 2_000_000;
  private static final int VALUE_LENGTH = 256;

  private static final String CLUSTER_PROPERTIES =
      """
      jobmanager.rpc.address: jobmanager
      pekko.ask.timeout: 60s
      taskmanager.numberOfTaskSlots: 4
      parallelism.default: 4
      taskmanager.memory.process.size: 4000m
      taskmanager.memory.managed.fraction: 0.2
      taskmanager.memory.jvm-overhead.min: 300m
      taskmanager.memory.jvm-overhead.max: 300m
      taskmanager.jvm-exit-on-oom: true
      jobmanager.memory.process.size: 1500m
      state.backend.type: rocksdb
      state.backend.rocksdb.memory.managed: true
      state.backend.rocksdb.memory.write-buffer-ratio: 0.5
      state.backend.rocksdb.predefined-options: FLASH_SSD_OPTIMIZED
      state.backend.rocksdb.thread.num: 4
      state.backend.rocksdb.checkpoint.transfer.thread.num: 4
      state.backend.rocksdb.timer-service.factory: ROCKSDB
      execution.checkpointing.interval: 1min
      execution.checkpointing.incremental: true
      execution.checkpointing.unaligned.enabled: true
      execution.checkpointing.dir: file:///tmp/checkpoints
      restart-strategy.type: none
      """;

  private final Network network = Network.newNetwork();
  private GenericContainer<?> jobManager;
  private GenericContainer<?> taskManager;

  @BeforeEach
  void startCluster() {
    assumeThat(DockerClientFactory.instance().getInfo().getArchitecture())
        .as("the jemalloc page size mismatch only exists on arm64")
        .isEqualTo("aarch64");

    jobManager =
        new GenericContainer<>(IMAGE)
            .withNetwork(network)
            .withNetworkAliases("jobmanager")
            .withEnv("FLINK_PROPERTIES", CLUSTER_PROPERTIES)
            .withCopyToContainer(Transferable.of(sql()), SQL_PATH)
            .withCommand("jobmanager");
    jobManager.start();

    taskManager =
        new GenericContainer<>(IMAGE)
            .withNetwork(network)
            .withEnv("FLINK_PROPERTIES", CLUSTER_PROPERTIES)
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.getHostConfig()
                        .withMemory(WARMUP_MEMORY_BYTES)
                        .withMemorySwap(WARMUP_MEMORY_BYTES)
                        .withCpusetCpus(TM_CPUSET))
            .withCommand("taskmanager");
    taskManager.start();
  }

  @AfterEach
  void stopCluster() {
    if (taskManager != null) {
      taskManager.stop();
    }
    if (jobManager != null) {
      jobManager.stop();
    }
    network.close();
  }

  @Test
  void givenRocksDbHeavyJob_whenRunningWithJemalloc_thenTaskManagerStaysWithinMemoryLimit()
      throws Exception {
    var submission = jobManager.execInContainer("sql-runner", "--sqlfile", SQL_PATH);
    assertThat(submission.getStdout())
        .withFailMessage(submission.getStdout() + submission.getStderr())
        .contains("Job has been submitted with JobID");

    await()
        .atMost(3, MINUTES)
        .pollInterval(5, SECONDS)
        .failFast("TaskManager died before the job started", () -> !taskManagerState().getRunning())
        .until(this::allTasksRunning);

    var runningSince = Instant.now();
    awaitTaskManagerAlive(runningSince, WARMUP);

    taskManager
        .getDockerClient()
        .updateContainerCmd(taskManager.getContainerId())
        .withMemory(STEADY_STATE_MEMORY_BYTES)
        .withMemorySwap(STEADY_STATE_MEMORY_BYTES)
        .exec();

    awaitTaskManagerAlive(runningSince, SURVIVAL_WINDOW);
  }

  private void awaitTaskManagerAlive(Instant runningSince, Duration window) {
    var until = Instant.now().plus(window);
    await()
        .atMost(window.plusMinutes(1))
        .pollInterval(5, SECONDS)
        .failFast(
            "TaskManager died while the job was running",
            () ->
                assertThat(taskManagerState().getRunning())
                    .withFailMessage(() -> describeDeath(runningSince))
                    .isTrue())
        .until(() -> Instant.now().isAfter(until));
  }

  private boolean allTasksRunning() throws Exception {
    var overview =
        jobManager.execInContainer("curl", "-s", "localhost:8081/jobs/overview").getStdout();
    var tasks = TASK_COUNTS.matcher(overview);
    return overview.contains("\"state\":\"RUNNING\"")
        && tasks.find()
        && tasks.group(1).equals(tasks.group(2));
  }

  private ContainerState taskManagerState() {
    return taskManager
        .getDockerClient()
        .inspectContainerCmd(taskManager.getContainerId())
        .exec()
        .getState();
  }

  private String describeDeath(Instant runningSince) {
    var state = taskManagerState();
    var logLines = taskManager.getLogs().lines().toList();
    return "TaskManager died %s after the job started: OOMKilled=%s exitCode=%s%n%s"
        .formatted(
            Duration.between(runningSince, Instant.now()),
            state.getOOMKilled(),
            state.getExitCodeLong(),
            String.join(
                "\n", logLines.subList(Math.max(0, logLines.size() - 50), logLines.size())));
  }

  private static String sql() {
    var inserts =
        IntStream.rangeClosed(1, STATEFUL_OPERATORS)
            .mapToObj(JemallocArm64OomIT::insert)
            .collect(Collectors.joining("\n"));
    return """
        SET 'table.exec.mini-batch.enabled' = 'false';

        CREATE TABLE src (k BIGINT, j BIGINT, s STRING) WITH (
          'connector' = 'datagen',
          'rows-per-second' = '%d',
          'fields.k.min' = '0',
          'fields.k.max' = '%d',
          'fields.j.min' = '0',
          'fields.j.max' = '1000000',
          'fields.s.length' = '%d'
        );

        CREATE TABLE sink_agg (k BIGINT, v STRING, w STRING, c BIGINT) WITH ('connector' = 'blackhole');
        CREATE TABLE sink_dedup (k BIGINT, j BIGINT, s STRING) WITH ('connector' = 'blackhole');

        EXECUTE STATEMENT SET
        BEGIN
        %s
        END;
        """
        .formatted(ROWS_PER_SECOND, KEYS, VALUE_LENGTH, inserts);
  }

  private static String insert(int i) {
    var len = (i * 37) % (VALUE_LENGTH - 6) + 4;
    if (i % 5 == 0) {
      return """
          INSERT INTO sink_dedup
          SELECT k, j, s FROM (
            SELECT k * %1$d AS k, j, SUBSTRING(s, 1, CAST(MOD(j, %2$d) AS INT) + 1) AS s,
                   ROW_NUMBER() OVER (PARTITION BY k * %1$d ORDER BY PROCTIME() DESC) AS rn
            FROM src
          ) WHERE rn = 1;
          """
          .formatted(i, len);
    }
    return """
        INSERT INTO sink_agg
        SELECT MOD(k + %1$d, %3$d),
               MAX(SUBSTRING(s, 1, CAST(MOD(j, %2$d) AS INT) + 1)),
               MIN(SUBSTRING(s, %1$d %% 16 + 1, CAST(MOD(j + %1$d, %2$d) AS INT) + 1)),
               COUNT(*)
        FROM src
        GROUP BY MOD(k + %1$d, %3$d);
        """
        .formatted(i, len, KEYS);
  }
}
