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
package com.datasqrl.flinkrunner.connector.pinot;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Version;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Integration test: Flink minicluster → Pinot OFFLINE segment sink.
 *
 * <p>Uses four containers (ZooKeeper + Controller + Broker + Server). Broker and server are needed
 * so that the controller's table-creation validator finds instances in {@code DefaultTenant}.
 * OFFLINE segment push itself only talks to the controller; broker/server are not contacted during
 * the write path.
 *
 * <p>All HTTP calls to Pinot use HTTP/1.1 explicitly: Pinot's Grizzly server does not support h2c
 * upgrade, and Java's default {@code HttpClient} (HTTP_2) stalls the server-side connection read
 * while negotiating the upgrade, causing a 30-second idle timeout on every POST.
 */
@Slf4j
@ExtendWith(MiniClusterExtension.class)
class PinotSinkFlinkTest {

  private static final String PINOT_IMAGE = "apachepinot/pinot:1.3.0";
  private static final int CONTROLLER_PORT = 9000;
  private static final String TABLE_NAME = "test_events";

  private static final Network NETWORK = Network.newNetwork();
  private static GenericContainer<?> ZOOKEEPER;
  private static GenericContainer<?> CONTROLLER;
  private static GenericContainer<?> BROKER;
  private static GenericContainer<?> SERVER;
  private static String controllerUrl;

  @BeforeAll
  static void startContainers() throws Exception {
    ZOOKEEPER =
        new GenericContainer<>("zookeeper:3.8")
            .withNetwork(NETWORK)
            .withNetworkAliases("zookeeper")
            .withExposedPorts(2181)
            // Wait until PrepRequestProcessor starts — that is when ZK is truly ready
            // to process client writes, not just listening on the port.
            .waitingFor(
                Wait.forLogMessage(".*PrepRequestProcessor.*started.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(1)));
    ZOOKEEPER.start();

    CONTROLLER =
        new GenericContainer<>(PINOT_IMAGE)
            .withNetwork(NETWORK)
            .withCommand(
                "StartController",
                "-controllerPort",
                String.valueOf(CONTROLLER_PORT),
                "-zkAddress",
                "zookeeper:2181",
                "-dataDir",
                "/tmp/pinotdata")
            .withExposedPorts(CONTROLLER_PORT)
            .withLogConsumer(frame -> System.out.print("CTRL| " + frame.getUtf8String()))
            // /health returns 200 only after the lead-controller resource is assigned in Helix.
            .waitingFor(
                Wait.forHttp("/health")
                    .forPort(CONTROLLER_PORT)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    CONTROLLER.start();

    // Broker and server register themselves via ZK; no fake instance registration needed.
    BROKER =
        new GenericContainer<>(PINOT_IMAGE)
            .withNetwork(NETWORK)
            .withCommand("StartBroker", "-zkAddress", "zookeeper:2181")
            .withLogConsumer(frame -> System.out.print("BRKR| " + frame.getUtf8String()));
    BROKER.start();

    SERVER =
        new GenericContainer<>(PINOT_IMAGE)
            .withNetwork(NETWORK)
            .withCommand("StartServer", "-zkAddress", "zookeeper:2181")
            .withLogConsumer(frame -> System.out.print("SRVR| " + frame.getUtf8String()));
    SERVER.start();

    controllerUrl =
        "http://" + CONTROLLER.getHost() + ":" + CONTROLLER.getMappedPort(CONTROLLER_PORT);

    // MiniCluster has NOT started yet (instance-level extension), so Helix can stabilise
    // without CPU competition. Wait for cluster readiness, then create schema+table.
    awaitClusterReady(controllerUrl);
    createPinotTable(controllerUrl);
  }

  @AfterAll
  static void stopContainers() {
    if (SERVER != null) SERVER.stop();
    if (BROKER != null) BROKER.stop();
    if (CONTROLLER != null) CONTROLLER.stop();
    if (ZOOKEEPER != null) ZOOKEEPER.stop();
    NETWORK.close();
  }

  @Test
  void writesRowsToOfflineTable() throws Exception {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    var tEnv = StreamTableEnvironment.create(env);

    tEnv.executeSql(
        "CREATE TABLE datagen_source ("
            + "  user_id  BIGINT,"
            + "  action   STRING,"
            + "  event_ts TIMESTAMP_LTZ(3)"
            + ") WITH ("
            + "  'connector'       = 'datagen',"
            + "  'number-of-rows'  = '20'"
            + ")");

    tEnv.executeSql(
        "CREATE TABLE pinot_sink ("
            + "  user_id  BIGINT,"
            + "  action   STRING,"
            + "  event_ts TIMESTAMP_LTZ(3)"
            + ") WITH ("
            + "  'connector'          = 'pinot',"
            + "  'controller.url'     = '"
            + controllerUrl
            + "',"
            + "  'table.name'         = '"
            + TABLE_NAME
            + "',"
            + "  'segment.flush.rows' = '10'"
            + ")");

    var result = tEnv.executeSql("INSERT INTO pinot_sink SELECT * FROM datagen_source");
    result.await();

    assertThat(result.getResultKind()).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT);

    // Verify at least one segment was uploaded to the controller.
    var segments = fetchSegments(controllerUrl);
    log.info("Segments in Pinot table {}: {}", TABLE_NAME, segments);
    assertThat(segments).contains(TABLE_NAME);
  }

  /**
   * Two-phase wait: (1) instances visible in /instances, (2) broker+server assigned to
   * DefaultTenant in Helix. Both checks use HTTP/1.1 to avoid h2c-upgrade stalls with Pinot's
   * Grizzly server.
   */
  private static void awaitClusterReady(String url) throws Exception {
    // Force HTTP/1.1: Pinot's Grizzly HTTP/1.1 server does not support h2c upgrade, and Java's
    // default HttpClient (HTTP_2) can stall the server-side connection read when issuing POSTs.
    var http = HttpClient.newBuilder().version(Version.HTTP_1_1).build();
    long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();

    // Phase 1: broker and server appear in /instances.
    while (System.currentTimeMillis() < deadline) {
      var resp =
          http.send(
              HttpRequest.newBuilder().uri(URI.create(url + "/instances")).GET().build(),
              BodyHandlers.ofString());
      String body = resp.body();
      if (resp.statusCode() == 200 && body.contains("Broker_") && body.contains("Server_")) {
        log.info("Phase 1 done — Pinot instances visible");
        break;
      }
      log.info("Phase 1: waiting for instances, body={}", body);
      Thread.sleep(5_000);
    }

    // Phase 2: DefaultTenant has broker + server assigned in Helix.
    // Response: {"BrokerInstances":["Broker_..."],"ServerInstances":["Server_..."],...}
    while (System.currentTimeMillis() < deadline) {
      var resp =
          http.send(
              HttpRequest.newBuilder()
                  .uri(URI.create(url + "/tenants/DefaultTenant/metadata"))
                  .GET()
                  .build(),
              BodyHandlers.ofString());
      String body = resp.body();
      log.info("Phase 2: tenant metadata={}", body);
      if (resp.statusCode() == 200 && body.contains("Broker_") && body.contains("Server_")) {
        log.info("Phase 2 done — DefaultTenant fully assigned");
        return;
      }
      Thread.sleep(5_000);
    }
    throw new AssertionError("Pinot cluster did not become ready within 3 minutes");
  }

  /** Creates the Pinot schema and OFFLINE table via the controller REST API. */
  private static void createPinotTable(String url) throws Exception {
    var http = HttpClient.newBuilder().version(Version.HTTP_1_1).build();

    var schema =
        """
        {
          "schemaName": "%s",
          "dimensionFieldSpecs": [
            { "name": "user_id",  "dataType": "LONG"   },
            { "name": "action",   "dataType": "STRING" },
            { "name": "event_ts", "dataType": "LONG"   }
          ]
        }
        """
            .formatted(TABLE_NAME);

    var schemaResp =
        http.send(
            HttpRequest.newBuilder()
                .uri(URI.create(url + "/schemas"))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString(schema))
                .build(),
            BodyHandlers.ofString());
    log.info("Create schema HTTP {}: {}", schemaResp.statusCode(), schemaResp.body());
    assertThat(schemaResp.statusCode()).isLessThan(300);

    var tableConfig =
        """
        {
          "tableName": "%s",
          "tableType": "OFFLINE",
          "segmentsConfig": {
            "replication": "1",
            "schemaName": "%s"
          },
          "ingestionConfig": {
            "batchIngestionConfig": {
              "segmentIngestionType": "APPEND",
              "segmentIngestionFrequency": "HOURLY",
              "batchConfigMaps": [
                {
                  "outputDirURI": "/tmp/pinotoutput",
                  "push.controllerUri": "%s"
                }
              ]
            }
          },
          "tenants": { "broker": "DefaultTenant", "server": "DefaultTenant" },
          "tableIndexConfig": { "loadMode": "MMAP" },
          "metadata": {}
        }
        """
            .formatted(TABLE_NAME, TABLE_NAME, url);

    var tableResp =
        http.send(
            HttpRequest.newBuilder()
                .uri(URI.create(url + "/tables"))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString(tableConfig))
                .build(),
            BodyHandlers.ofString());
    log.info("Create table HTTP {}: {}", tableResp.statusCode(), tableResp.body());
    assertThat(tableResp.statusCode()).isLessThan(300);
  }

  private static String fetchSegments(String url) throws Exception {
    var http = HttpClient.newBuilder().version(Version.HTTP_1_1).build();
    var request =
        HttpRequest.newBuilder().uri(URI.create(url + "/segments/" + TABLE_NAME)).GET().build();
    return http.send(request, BodyHandlers.ofString()).body();
  }
}
