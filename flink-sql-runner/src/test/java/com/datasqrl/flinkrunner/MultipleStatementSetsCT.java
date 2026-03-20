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
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * K8s-based E2E test that verifies multiple EXECUTE STATEMENT SET blocks work correctly when
 * submitted via FlinkSessionJob (Flink Operator session mode).
 *
 * <p>This test requires:
 *
 * <ul>
 *   <li>kubectl configured with access to a K8s cluster with the Flink operator installed
 *   <li>The flink-sql-runner image accessible from the cluster
 * </ul>
 *
 * <p>Run with: {@code mvn verify -Dit.test=MultipleStatementSetsCT
 * -Dflink.runner.image=<ecr-image>:<tag>}
 */
@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultipleStatementSetsCT {

  private static final String NAMESPACE_PREFIX = "flink-runner-ct-";
  private static final String FLINK_DEPLOYMENT_NAME = "flink-application-test-0";
  private static final String SESSION_JOB_NAME = "flink-session-job-test-0";

  private static final String DEFAULT_IMAGE =
      "819957634547.dkr.ecr.us-east-1.amazonaws.com/cache/dockerhub/datasqrl/flink-sql-runner:0.9.5-flink-2.2";

  private String flinkImage;
  private String namespace;

  @BeforeAll
  void setUp() throws Exception {
    flinkImage = System.getProperty("flink.runner.image", DEFAULT_IMAGE);
    namespace = NAMESPACE_PREFIX + Long.toHexString(System.currentTimeMillis());
    log.info("Using Flink image: {}", flinkImage);
    log.info("Using namespace: {}", namespace);

    kubectl("create", "namespace", namespace);

    kubectl("create", "serviceaccount", "flink", "-n", namespace);

    kubectl(
        "create",
        "rolebinding",
        "flink-edit",
        "-n",
        namespace,
        "--clusterrole=edit",
        "--serviceaccount=" + namespace + ":flink");

    applyFlinkSqlConfigMap();
    applyHadoopConfigMap();
    applyFlinkDeployment();
    applyFlinkSessionJob();

    kubectl(
        "annotate",
        "flinkdeployment",
        FLINK_DEPLOYMENT_NAME,
        "-n",
        namespace,
        "force-reconcile=" + System.currentTimeMillis(),
        "--overwrite");

    log.info("All resources applied, waiting for pods...");
  }

  @AfterAll
  void tearDown() {
    try {
      collectDiagnostics();
      cleanupNamespace();
    } catch (Exception e) {
      log.warn("Cleanup failed", e);
    }
  }

  @Test
  void given_multipleStatementSets_when_sessionMode_then_allStatementSetsComplete()
      throws Exception {
    await("first-statement-set-completed")
        .atMost(5, MINUTES)
        .pollInterval(10, SECONDS)
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              var pods = kubectl("get", "pods", "-n", namespace, "--no-headers");
              log.info("Pods:\n{}", pods);
              var tmLogs = getTaskManagerLogs();
              assertThat(tmLogs).contains("PrintOutputA");
            });

    log.info("First statement set completed, checking for second...");

    var tmLogs = getTaskManagerLogs();

    assertThat(tmLogs)
        .as("First statement set output (PrintOutputA) should be present")
        .contains("PrintOutputA");

    assertThat(tmLogs)
        .as("Second statement set output (PrintOutputB) should be present")
        .contains("PrintOutputB");
  }

  private String getTaskManagerLogs() throws Exception {
    var tmPod =
        kubectl(
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                "component=taskmanager",
                "-o",
                "jsonpath={.items[0].metadata.name}")
            .trim();

    if (tmPod.isEmpty()) {
      var allPods = kubectl("get", "pods", "-n", namespace, "--no-headers");
      var tmLine = allPods.lines().filter(l -> l.contains("taskmanager")).findFirst().orElse("");
      tmPod = tmLine.split("\\s+")[0];
    }

    assertThat(tmPod).as("TaskManager pod should exist").isNotEmpty();
    return kubectl("logs", tmPod, "-n", namespace, "-c", "flink-main-container");
  }

  private void applyFlinkSqlConfigMap() throws Exception {
    var yaml =
        """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: flink-sql-test
          namespace: %s
        data:
          config.yaml: |
            "table.exec.source.idle-timeout": "1 ms"
            "execution.runtime-mode": "BATCH"
          flink.sql: |
            CREATE TABLE `InA` (
              `id` BIGINT,
              `name` STRING
            ) WITH (
              'connector' = 'datagen',
              'number-of-rows' = '10'
            );
            CREATE TABLE `InB` (
              `id` BIGINT,
              `name` STRING
            ) WITH (
              'connector' = 'datagen',
              'number-of-rows' = '10'
            );
            CREATE TABLE `OutA` (
              `id` BIGINT,
              `name` STRING
            ) WITH (
              'connector' = 'print',
              'print-identifier' = 'PrintOutputA'
            );
            CREATE TABLE `OutB` (
              `id` BIGINT,
              `name` STRING
            ) WITH (
              'connector' = 'print',
              'print-identifier' = 'PrintOutputB'
            );
            EXECUTE STATEMENT SET BEGIN
            INSERT INTO `OutA`
            SELECT *
             FROM `InA`
             ORDER BY `id`
            ;
            END;
            EXECUTE STATEMENT SET BEGIN
            INSERT INTO `OutB`
            SELECT *
             FROM `InB`
             ORDER BY `id`
            ;
            END
        """
            .formatted(namespace);
    kubectlApply(yaml);
  }

  private void applyHadoopConfigMap() throws Exception {
    var yaml =
        """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: hadoop-config-%s
          namespace: %s
        data:
          core-site.xml: |
            <configuration>
            </configuration>
        """
            .formatted(FLINK_DEPLOYMENT_NAME, namespace);
    kubectlApply(yaml);
  }

  private void applyFlinkDeployment() throws Exception {
    var yaml =
        """
        apiVersion: flink.apache.org/v1beta1
        kind: FlinkDeployment
        metadata:
          name: %s
          namespace: %s
        spec:
          image: %s
          flinkVersion: v2_2
          flinkConfiguration:
            taskmanager.numberOfTaskSlots: "1"
            taskmanager.memory.process.size: "2000m"
            jobmanager.memory.process.size: "1000m"
            execution.runtime-mode: "BATCH"
            pipeline.classpaths: "file:///opt/flink/flink-sql-runner.jar"
            security.delegation.tokens.enabled: "false"
          serviceAccount: flink
          jobManager:
            replicas: 1
            resource:
              cpu: 0.5
          taskManager:
            replicas: 1
            resource:
              cpu: 0.5
          podTemplate:
            spec:
              nodeSelector:
                sqrlpipeline/tier: standard
                sqrlpipeline/type: sqrlpipeline
              containers:
                - name: flink-main-container
                  volumeMounts:
                    - name: flink-sql
                      mountPath: /opt/flink/usrlib/flink-files
                    - name: udf-jars
                      mountPath: /opt/flink/usrlib/flink-files/jars
              volumes:
                - name: flink-sql
                  configMap:
                    name: flink-sql-test
                - name: udf-jars
                  emptyDir: {}
        """
            .formatted(FLINK_DEPLOYMENT_NAME, namespace, flinkImage);
    kubectlApply(yaml);
  }

  private void applyFlinkSessionJob() throws Exception {
    var yaml =
        """
        apiVersion: flink.apache.org/v1beta1
        kind: FlinkSessionJob
        metadata:
          name: %s
          namespace: %s
        spec:
          deploymentName: %s
          job:
            entryClass: com.datasqrl.flinkrunner.CliRunner
            args:
              - "--sqlfile"
              - "/opt/flink/usrlib/flink-files/flink.sql"
              - "--config-dir"
              - "/opt/flink/usrlib/flink-files"
              - "--udfpath"
              - "/opt/flink/usrlib/flink-files/jars"
            parallelism: 1
            upgradeMode: stateless
        """
            .formatted(SESSION_JOB_NAME, namespace, FLINK_DEPLOYMENT_NAME);
    kubectlApply(yaml);
  }

  private void cleanupNamespace() {
    try {
      kubectl(
          "patch",
          "flinkdeployment",
          FLINK_DEPLOYMENT_NAME,
          "-n",
          namespace,
          "-p",
          "{\"metadata\":{\"finalizers\":null}}",
          "--type=merge");
    } catch (Exception ignored) {
    }
    try {
      kubectl(
          "patch",
          "flinksessionjob",
          SESSION_JOB_NAME,
          "-n",
          namespace,
          "-p",
          "{\"metadata\":{\"finalizers\":null}}",
          "--type=merge");
    } catch (Exception ignored) {
    }
    try {
      kubectl("delete", "namespace", namespace, "--force", "--grace-period=0", "--wait=false");
      await("namespace-deleted")
          .atMost(60, SECONDS)
          .pollInterval(2, SECONDS)
          .ignoreExceptions()
          .untilAsserted(
              () -> {
                var ns = kubectl("get", "namespace", namespace, "--ignore-not-found");
                assertThat(ns.trim()).isEmpty();
              });
    } catch (Exception ignored) {
    }
  }

  private void collectDiagnostics() {
    try {
      log.info("=== Diagnostics ===");
      log.info(
          "FlinkDeployment status:\n{}",
          kubectl("get", "flinkdeployment", FLINK_DEPLOYMENT_NAME, "-n", namespace, "-o", "yaml"));
      log.info(
          "FlinkSessionJob status:\n{}",
          kubectl("get", "flinksessionjob", SESSION_JOB_NAME, "-n", namespace, "-o", "yaml"));
      log.info("Pods:\n{}", kubectl("get", "pods", "-n", namespace, "-o", "wide"));
      log.info(
          "Events:\n{}", kubectl("get", "events", "-n", namespace, "--sort-by=.lastTimestamp"));
    } catch (Exception e) {
      log.warn("Failed to collect diagnostics", e);
    }
  }

  private String kubectl(String... args) throws Exception {
    var cmd = new String[args.length + 1];
    cmd[0] = "kubectl";
    System.arraycopy(args, 0, cmd, 1, args.length);
    return exec(cmd);
  }

  private void kubectlApply(String yaml) throws Exception {
    var process =
        new ProcessBuilder("kubectl", "apply", "-f", "-").redirectErrorStream(true).start();
    process.getOutputStream().write(yaml.getBytes(StandardCharsets.UTF_8));
    process.getOutputStream().close();
    var output =
        new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    var exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException("kubectl apply failed: " + output);
    }
    log.info("Applied: {}", output);
  }

  private String exec(String... cmd) throws Exception {
    var process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
    var output =
        new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
    var exitCode = process.waitFor();
    if (exitCode != 0) {
      throw new RuntimeException(
          "Command failed (exit " + exitCode + "): " + String.join(" ", cmd) + "\n" + output);
    }
    return output;
  }
}
