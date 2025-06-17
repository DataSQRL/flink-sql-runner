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
package com.datasqrl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.model.JobStatus;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Slf4j
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractITSupport {

  protected static final String CONTAINER_TEST_OUT_PATH = "/tmp/test-out";
  protected static final int FLINK_PORT = 8081;
  protected static final int REDPANDA_PORT = 9092;

  private static final Network sharedNetwork = Network.newNetwork();

  @Container
  protected static final GenericContainer<?> postgresContainer =
      new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases("postgres")
          .withDatabaseName("datasqrl")
          .withUsername("postgres")
          .withPassword("postgres")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("sqrl/postgres-schema.sql"),
              "/docker-entrypoint-initdb.d/init.sql");

  @Container
  protected static final GenericContainer<?> redpandaContainer =
      new GenericContainer<>(DockerImageName.parse("redpandadata/redpanda"))
          .withNetwork(sharedNetwork)
          .withNetworkAliases("redpanda")
          .withExposedPorts(REDPANDA_PORT);

  protected static GenericContainer<?> flinkContainer;

  protected FlinkApi client;

  @SuppressWarnings("resource")
  @BeforeAll
  protected void init() throws Exception {
    int redisPort = redpandaContainer.getMappedPort(REDPANDA_PORT);

    flinkContainer =
        new GenericContainer<>(DockerImageName.parse("flink-sql-runner"))
            .withNetwork(sharedNetwork)
            .withExposedPorts(FLINK_PORT)
            .withEnv("JDBC_URL", "jdbc:postgresql://postgres:5432/datasqrl")
            .withEnv("JDBC_USERNAME", "postgres")
            .withEnv("JDBC_PASSWORD", "postgres")
            .withEnv("REDPANDA_PORT", String.valueOf(redisPort))
            .withFileSystemBind("target/test-classes/plans", "/it/planfile", BindMode.READ_ONLY)
            .withFileSystemBind("target/test-classes/sql", "/it/sqlfile", BindMode.READ_ONLY)
            .withFileSystemBind("target/test-classes/sqrl", "/it/sqrl", BindMode.READ_ONLY)
            .withFileSystemBind("target/test-classes/config", "/it/config", BindMode.READ_ONLY)
            .withFileSystemBind("target/test-classes/udfs", "/it/udfs", BindMode.READ_ONLY)
            .withCommand("bash", "-c", "bin/start-cluster.sh && tail -f /dev/null")
            .dependsOn(postgresContainer, redpandaContainer);
    flinkContainer.start();

    client = createClient();
  }

  @AfterAll
  protected void teardown() {
    flinkContainer.stop();
  }

  protected FlinkApi createClient() throws ApiException {
    var serverUrl = "http://localhost:" + flinkContainer.getMappedPort(FLINK_PORT);
    var client = new FlinkApi();
    client.getApiClient().setBasePath(serverUrl);
    client
        .getApiClient()
        .setHttpClient(
            client
                .getApiClient()
                .getHttpClient()
                .newBuilder()
                .connectTimeout(Duration.ofMinutes(2))
                .writeTimeout(Duration.ofMinutes(2))
                .readTimeout(Duration.ofMinutes(2))
                .build());

    await()
        .atMost(100, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .ignoreExceptions()
        .until(() -> client.getJobsOverview() != null);

    final var statusOverview = client.getJobIdsWithStatusesOverview();
    statusOverview
        .getJobs()
        .forEach(
            jobIdWithStatus -> {
              try {
                client.cancelJob(jobIdWithStatus.getId(), TerminationMode.CANCEL);
              } catch (ApiException ignored) {
              }
            });

    return client;
  }

  protected String flinkRun(String... sqlRunnerArgs) throws Exception {
    return flinkRun(List.of(sqlRunnerArgs), null);
  }

  protected String flinkRun(List<String> sqlRunnerArgs) throws Exception {
    return flinkRun(sqlRunnerArgs, null);
  }

  protected String flinkRun(List<String> sqlRunnerArgs, @Nullable String savepointPath)
      throws Exception {
    var execCmd = new ArrayList<>(List.of("flink", "run"));

    if (savepointPath != null) {
      execCmd.add("-s");
      execCmd.add(savepointPath);
    }

    execCmd.add("./plugins/flink-sql-runner/flink-sql-runner.uber.jar");
    execCmd.addAll(sqlRunnerArgs);

    var execRes = flinkContainer.execInContainer(execCmd.toArray(new String[0]));
    var stdOut = execRes.getStdout();
    var stdErr = execRes.getStderr();

    assertThat(stdErr)
        .withFailMessage(stdErr)
        .doesNotContain("The program finished with the following exception:");

    var jobIdOpt =
        stdOut
            .lines()
            .filter(line -> line.startsWith("Job has been submitted with JobID"))
            .map(line -> line.substring(line.lastIndexOf(" ") + 1).trim())
            .findFirst();

    assertThat(jobIdOpt).as("Failed to fetch JobID").isPresent();

    return jobIdOpt.get();
  }

  protected void assertJobIsRunning(String jobId) throws ApiException {
    var jobStatus = client.getJobStatusInfo(jobId);

    await()
        .during(5, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .until(() -> jobStatus.getStatus() == JobStatus.RUNNING);
  }

  protected void untilAssert(ThrowingRunnable assertion) {
    await()
        .atMost(20, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .ignoreExceptions()
        .untilAsserted(assertion);
  }

  protected void copyDirectoryFromContainer(
      GenericContainer<?> container, String containerPath, Path targetDir) throws Exception {
    try (var copyCmd =
            container
                .getDockerClient()
                .copyArchiveFromContainerCmd(container.getContainerId(), containerPath);
        var is = copyCmd.exec()) {
      try (TarArchiveInputStream tarInput = new TarArchiveInputStream(is)) {
        ArchiveEntry entry;
        while ((entry = tarInput.getNextEntry()) != null) {
          Path outputPath = targetDir.resolve(entry.getName());
          if (entry.isDirectory()) {
            Files.createDirectories(outputPath);
          } else {
            Files.createDirectories(outputPath.getParent());
            Files.copy(tarInput, outputPath, StandardCopyOption.REPLACE_EXISTING);
          }
        }
      }
    }
  }
}
