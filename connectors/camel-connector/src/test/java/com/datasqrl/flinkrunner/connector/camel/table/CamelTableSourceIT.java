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
package com.datasqrl.flinkrunner.connector.camel.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.Csv;
import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.camel.Json;
import com.datasqrl.flinkrunner.camel.Request;
import com.datasqrl.flinkrunner.connector.camel.source.TestHttpServer;
import com.datasqrl.flinkrunner.connector.camel.table.RecordTypeTest.Everything;
import com.datasqrl.flinkrunner.connector.camel.table.RecordTypeTest.Inner;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.camel.Exchange;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the {@code camel} SQL connector over each mode. A source class is named by the DDL and built
 * with its no-argument constructor, so the test hands it its location through a static field.
 */
class CamelTableSourceIT {

  private static final String FAST_FETCH = "'fetch.timeout' = '200ms'";

  @TempDir Path dir;
  private TestHttpServer api;

  @BeforeEach
  void pointSourcesAtTheTestDirectory() {
    JsonInbox.endpoint = "file:" + dir + "?delete=true&delay=10";
    CsvFiles.location = "file:" + dir + "?include=.*\\.csv";
    EverythingFile.location = "file:" + dir;
  }

  @AfterEach
  void stopApi() {
    if (api != null) {
      api.close();
    }
  }

  // --- ack mode

  @Test
  void ackTableReadsRecordsAndMetadata() throws Exception {
    writeJson("a", "{\"id\": 1, \"name\": \"alice\"}");
    writeJson("b", "{\"id\": 2, \"name\": \"bob\"}");
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql(
        "CREATE TABLE people ("
            + "  id INT,"
            + "  name STRING,"
            + "  ingested_at TIMESTAMP_LTZ(3) METADATA,"
            + "  exchange_id STRING METADATA,"
            + "  file_headers MAP<STRING, STRING> METADATA FROM 'headers'"
            + ") WITH ("
            + ackOptions()
            + ")");

    List<Row> rows =
        collect(
            tEnv,
            "SELECT id, name, ingested_at, exchange_id, file_headers['CamelFileName'] FROM people");

    assertThat(rows)
        .extracting(row -> row.getField(0), row -> row.getField(1))
        .containsExactlyInAnyOrder(tuple(1, "alice"), tuple(2, "bob"));
    assertThat(rows)
        .extracting(row -> row.getField(4))
        .containsExactlyInAnyOrder("a.json", "b.json");
    assertThat(rows).allSatisfy(row -> assertThat(row.getField(2)).isNotNull());
    assertThat(rows).allSatisfy(row -> assertThat(row.getField(3)).isNotNull());
    awaitEmptyDirectory();
  }

  @Test
  void ackTableAcknowledgesRecordsThatFailToMap() throws Exception {
    writeJson("good", "{\"id\": 1}");
    writeJson("bad", "not json");
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql("CREATE TABLE people (id INT, name STRING) WITH (" + ackOptions() + ")");

    assertThat(collect(tEnv, "SELECT id, name FROM people")).containsExactly(Row.of(1, null));
    awaitEmptyDirectory();
  }

  @Test
  void emitsAWatermarkAfterEachPoll() throws Exception {
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql(
        "CREATE TABLE people ("
            + "  id INT,"
            + "  name STRING,"
            + "  ingested_at TIMESTAMP_LTZ(3) METADATA,"
            + "  WATERMARK FOR ingested_at AS SOURCE_WATERMARK()"
            + ") WITH ("
            + "  'connector' = 'camel',"
            + "  'source.class' = '"
            + JsonInbox.class.getName()
            + "',"
            + FAST_FETCH
            + ")");

    writeJson("first", "{\"id\": 1}");
    try (CloseableIterator<Row> results =
        tEnv.executeSql("SELECT id, ingested_at, CURRENT_WATERMARK(ingested_at) FROM people")
            .collect()) {
      Row first = results.next();
      assertThat(first.getField(0)).isEqualTo(1);
      assertThat(first.getField(2)).as("no poll had completed before the first record").isNull();

      writeJson("second", "{\"id\": 2}");
      Row second = results.next();
      assertThat(second.getField(0)).isEqualTo(2);
      Instant firstPoll = (Instant) first.getField(1);
      assertThat((Instant) second.getField(1)).isAfter(firstPoll);
      assertThat(second.getField(2)).as("the first poll's watermark").isEqualTo(firstPoll);
    }
  }

  @Test
  void ackTableRejectsBatchMode() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    tEnv.executeSql("CREATE TABLE people (id INT, name STRING) WITH (" + ackOptions() + ")");

    assertPlanningFails(tEnv, "SELECT id FROM people", ValidationException.class, "scan.bounded");
  }

  @Test
  void requiresSourceClass() {
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.executeSql("CREATE TABLE people (id INT, name STRING) WITH ('connector' = 'camel')");

    assertPlanningFails(tEnv, "SELECT id FROM people", ValidationException.class, "source.class");
  }

  @Test
  void componentOptionsReachTheComponent() throws Exception {
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql(
        "CREATE TABLE ticks (counter STRING) WITH ("
            + "  'connector' = 'camel',"
            + "  'source.class' = '"
            + TimerTicks.class.getName()
            + "',"
            + "  'camel.component.timer.includeMetadata' = 'true',"
            + "  'scan.bounded' = 'true',"
            + FAST_FETCH
            + ")");

    assertThat(collect(tEnv, "SELECT counter FROM ticks")).containsExactly(Row.of("1"));
  }

  // --- cursor mode

  @Test
  void cursorTableReadsEveryPage() throws Exception {
    startApi();
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql(
        "CREATE TABLE items (id BIGINT, ts BIGINT, ingested_at TIMESTAMP_LTZ(3) METADATA) WITH ("
            + cursorOptions()
            + ", 'scan.bounded' = 'true')");

    List<Row> rows = collect(tEnv, "SELECT id, ingested_at FROM items");

    assertThat(rows).extracting(row -> row.getField(0)).contains(1L, 2L, 3L);
    assertThat(rows).allSatisfy(row -> assertThat(row.getField(1)).isNotNull());
  }

  @Test
  void rejectsAnOptionOfAnotherMode() throws Exception {
    startApi();
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.executeSql(
        "CREATE TABLE items (id BIGINT, ts BIGINT) WITH ("
            + cursorOptions()
            + ", 'file.order' = 'mtime')");

    assertPlanningFails(tEnv, "SELECT id FROM items", ValidationException.class, "'file.order'");
  }

  @Test
  void rejectsMetadataOfAnotherMode() throws Exception {
    startApi();
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.executeSql(
        "CREATE TABLE items (id BIGINT, ts BIGINT, file_name STRING METADATA FROM 'file') WITH ("
            + cursorOptions()
            + ")");

    assertPlanningFails(tEnv, "SELECT id FROM items", ValidationException.class, "file");
  }

  @Test
  void rejectsAColumnTheRecordLacks() throws Exception {
    startApi();
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.executeSql(
        "CREATE TABLE items (id BIGINT, ts BIGINT, extra STRING) WITH (" + cursorOptions() + ")");

    assertPlanningFails(tEnv, "SELECT id FROM items", ValidationException.class, "'extra'");
  }

  @Test
  void rejectsAPrimaryKey() throws Exception {
    startApi();
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    tEnv.executeSql(
        "CREATE TABLE items (id BIGINT NOT NULL, ts BIGINT, PRIMARY KEY (id) NOT ENFORCED) WITH ("
            + cursorOptions()
            + ")");

    assertPlanningFails(tEnv, "SELECT id FROM items", ValidationException.class, "primary key");
  }

  // --- file mode

  @Test
  void fileTableExposesFileAndRow() throws Exception {
    Files.writeString(dir.resolve("a.csv"), "x\ny\n");
    Files.writeString(dir.resolve("b.csv"), "z\n");
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql(
        "CREATE TABLE lines ("
            + "  id STRING,"
            + "  file_name STRING METADATA FROM 'file',"
            + "  row_num BIGINT METADATA FROM 'row'"
            + ") WITH ("
            + fileOptions(CsvFiles.class)
            + ", 'scan.bounded' = 'true')");

    assertThat(collect(tEnv, "SELECT file_name, row_num FROM lines"))
        .containsExactlyInAnyOrder(Row.of("a.csv", 1L), Row.of("a.csv", 2L), Row.of("b.csv", 1L));
  }

  @Test
  void cursorAndFileTablesFinishInBatchMode() throws Exception {
    startApi();
    Files.writeString(dir.resolve("a.csv"), "x\n");
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    tEnv.executeSql("CREATE TABLE items (id BIGINT, ts BIGINT) WITH (" + cursorOptions() + ")");
    tEnv.executeSql("CREATE TABLE lines (id STRING) WITH (" + fileOptions(CsvFiles.class) + ")");

    assertThat(collect(tEnv, "SELECT id FROM items"))
        .extracting(row -> row.getField(0))
        .contains(1L, 2L, 3L);
    assertThat(collect(tEnv, "SELECT id FROM lines")).containsExactly(Row.of("x"));
  }

  @Test
  void mapsEveryDocumentedTypeAndNulls() throws Exception {
    Files.writeString(dir.resolve("records.txt"), "full\nnulls\n");
    StreamTableEnvironment tEnv = streamingTableEnv();
    tEnv.executeSql(
        "CREATE TABLE everything ("
            + "  text STRING,"
            + "  flag BOOLEAN,"
            + "  small INT,"
            + "  big BIGINT,"
            + "  ratio DOUBLE,"
            + "  amount DECIMAL(18, 2),"
            + "  `day` DATE,"
            + "  `local` TIMESTAMP(3),"
            + "  moment TIMESTAMP_LTZ(3),"
            + "  tags ARRAY<STRING>,"
            + "  counts MAP<STRING, BIGINT>,"
            + "  `inner` ROW<x STRING>"
            + ") WITH ("
            + fileOptions(EverythingFile.class)
            + ", 'scan.bounded' = 'true')");

    List<Row> rows = collect(tEnv, "SELECT * FROM everything");

    assertThat(rows).hasSize(2);
    Row full = rows.stream().filter(row -> row.getField(0) != null).findFirst().orElseThrow();
    Row nulls = rows.stream().filter(row -> row.getField(0) == null).findFirst().orElseThrow();
    Everything expected = EverythingFile.full();
    assertThat(fieldsOf(full))
        .containsExactly(
            expected.text(),
            expected.flag(),
            expected.small(),
            expected.big(),
            expected.ratio(),
            expected.amount(),
            expected.day(),
            expected.local(),
            expected.moment(),
            expected.tags().toArray(new String[0]),
            expected.counts(),
            Row.of(expected.inner().x()));
    List<Object> nullFields = fieldsOf(nulls);
    assertThat(nullFields.get(0)).isNull();
    assertThat(nullFields.get(5)).isNull();
    assertThat(nullFields.subList(1, 5)).containsExactly(true, 1, 2L, 0.5);
    assertThat(nullFields.subList(6, 9))
        .containsExactly(expected.day(), expected.local(), expected.moment());
  }

  // --- sources

  public record Person(Integer id, String name) {}

  /** JSON files in the test directory, deleted once acknowledged. */
  public static final class JsonInbox implements AckSource<Person> {
    static volatile String endpoint;
    private final String uri = endpoint;

    @Override
    public String endpoint() {
      return uri;
    }

    @Override
    public Person map(Exchange exchange) {
      JsonNode json = Json.body(exchange);
      return new Person(json.get("id").asInt(), json.path("name").asText(null));
    }
  }

  public record Tick(String counter) {}

  /** Fires once; the counter is only set when the timer includes its metadata. */
  public static final class TimerTicks implements AckSource<Tick> {
    @Override
    public String endpoint() {
      return "timer:tick?repeatCount=1&delay=0";
    }

    @Override
    public Tick map(Exchange exchange) {
      return new Tick(String.valueOf(exchange.getProperty(Exchange.TIMER_COUNTER)));
    }
  }

  public record Item(Long id, Long ts) {}

  /** The test API: rows {@code id,ts} with {@code ts >= since}, two per page. */
  public static final class PagedApi implements CursorSource<Item, Long, String> {
    static volatile String url;
    private final String api = url;

    @Override
    public Long initial() {
      return 0L;
    }

    @Override
    public Request request(Long since) {
      return Request.to(api).header(Exchange.HTTP_QUERY, "since=" + since);
    }

    @Override
    public Optional<Request> next(Exchange response) {
      return Optional.ofNullable(response.getMessage().getHeader("X-Next-Page", String.class))
          .map(query -> Request.to(api).header(Exchange.HTTP_QUERY, query));
    }

    @Override
    public Iterable<String> records(Exchange response) {
      String body = response.getMessage().getBody(String.class);
      return body == null || body.isEmpty() ? List.of() : List.of(body.split("\n"));
    }

    @Override
    public Item map(String row) {
      String[] fields = row.split(",");
      return new Item(Long.parseLong(fields[0]), Long.parseLong(fields[1]));
    }

    @Override
    public Long position(Item item) {
      return item.ts();
    }
  }

  public record Line(String id) {}

  /** CSV files in the test directory, one record per line. */
  public static final class CsvFiles implements FileSource<Line, List<String>> {
    static volatile String location;
    private final String uri = location;

    @Override
    public String location() {
      return uri;
    }

    @Override
    public Iterable<List<String>> records(Exchange file) {
      return Csv.parse(file, Csv.NO_HEADER);
    }

    @Override
    public Line map(List<String> fields) {
      return new Line(fields.get(0));
    }
  }

  /** A file whose line {@code full} sets every field, and {@code nulls} leaves two null. */
  public static final class EverythingFile implements FileSource<Everything, String> {
    static volatile String location;
    private final String uri = location;

    static Everything full() {
      return new Everything(
          "text",
          true,
          1,
          2L,
          0.5,
          new BigDecimal("12.34"),
          LocalDate.of(2026, 11, 19),
          LocalDateTime.of(2026, 11, 19, 10, 15, 30),
          Instant.parse("2026-11-19T10:15:30.123Z"),
          List.of("a", "b"),
          Map.of("k", 7L),
          new Inner("x"));
    }

    @Override
    public String location() {
      return uri;
    }

    @Override
    public Iterable<String> records(Exchange file) {
      return file.getMessage().getBody(String.class).lines().toList();
    }

    @Override
    public Everything map(String line) {
      Everything full = full();
      return "full".equals(line)
          ? full
          : new Everything(
              null,
              full.flag(),
              full.small(),
              full.big(),
              full.ratio(),
              null,
              full.day(),
              full.local(),
              full.moment(),
              full.tags(),
              full.counts(),
              full.inner());
    }
  }

  // --- helpers

  private static String ackOptions() {
    return "'connector' = 'camel', 'source.class' = '"
        + JsonInbox.class.getName()
        + "', 'scan.bounded' = 'true', "
        + FAST_FETCH;
  }

  private static String cursorOptions() {
    return "'connector' = 'camel', 'source.class' = '"
        + PagedApi.class.getName()
        + "', 'retry.max-attempts' = '0', "
        + FAST_FETCH;
  }

  private static String fileOptions(Class<?> sourceClass) {
    return "'connector' = 'camel', 'source.class' = '"
        + sourceClass.getName()
        + "', 'file.ready' = 'none', "
        + FAST_FETCH;
  }

  private void startApi() throws IOException {
    api =
        new TestHttpServer(
            "/items",
            request -> {
              long since = Long.parseLong(param(request.query(), "since"));
              int page =
                  Integer.parseInt(Optional.ofNullable(param(request.query(), "page")).orElse("0"));
              List<String> rows =
                  Stream.of("1,10", "2,20", "3,30")
                      .filter(row -> Long.parseLong(row.split(",")[1]) >= since)
                      .toList();
              int from = Math.min(page * 2, rows.size());
              int to = Math.min(from + 2, rows.size());
              Map<String, String> headers =
                  to < rows.size()
                      ? Map.of("X-Next-Page", "since=" + since + "&page=" + (page + 1))
                      : Map.of();
              return new TestHttpServer.Response(
                  200, String.join("\n", rows.subList(from, to)), headers);
            });
    PagedApi.url = api.url("/items");
  }

  private static String param(String query, String name) {
    for (String pair : query.split("&")) {
      String[] keyValue = pair.split("=");
      if (keyValue[0].equals(name)) {
        return keyValue[1];
      }
    }
    return null;
  }

  private static StreamTableEnvironment streamingTableEnv() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.enableCheckpointing(50);
    return StreamTableEnvironment.create(env);
  }

  private static List<Row> collect(TableEnvironment tEnv, String query) throws Exception {
    try (CloseableIterator<Row> results = tEnv.executeSql(query).collect()) {
      return CollectionUtil.iteratorToList(results);
    }
  }

  private static void assertPlanningFails(
      TableEnvironment tEnv, String query, Class<? extends Throwable> type, String message) {
    assertThatThrownBy(() -> tEnv.explainSql(query))
        .satisfies(
            e ->
                assertThat(
                        ExceptionUtils.findThrowable(
                            e,
                            t ->
                                type.isInstance(t)
                                    && String.valueOf(t.getMessage()).contains(message)))
                    .as(ExceptionUtils.stringifyException(e))
                    .isPresent());
  }

  private static List<Object> fieldsOf(Row row) {
    List<Object> fields = new ArrayList<>();
    for (int i = 0; i < row.getArity(); i++) {
      fields.add(row.getField(i));
    }
    return fields;
  }

  private void writeJson(String name, String content) throws IOException {
    // Write then move, so the file component never picks up a half-written file.
    Path tmp = Files.writeString(dir.resolveSibling(dir.getFileName() + "-" + name), content);
    Files.move(tmp, dir.resolve(name + ".json"));
  }

  private void awaitEmptyDirectory() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (true) {
      try (Stream<Path> files = Files.list(dir)) {
        Set<Path> left = files.filter(Files::isRegularFile).collect(Collectors.toSet());
        if (left.isEmpty()) {
          return;
        }
        if (System.nanoTime() > deadline) {
          fail("Files were never acknowledged: " + left);
        }
      }
      Thread.sleep(50);
    }
  }
}
