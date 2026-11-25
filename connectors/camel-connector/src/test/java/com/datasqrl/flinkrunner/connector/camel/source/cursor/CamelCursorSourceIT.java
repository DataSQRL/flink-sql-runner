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
package com.datasqrl.flinkrunner.connector.camel.source.cursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.Request;
import com.datasqrl.flinkrunner.connector.camel.source.FailOnceAfterCheckpoint;
import com.datasqrl.flinkrunner.connector.camel.source.PolledRecords;
import com.datasqrl.flinkrunner.connector.camel.source.PolledRecords.Polled;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import com.datasqrl.flinkrunner.connector.camel.source.TestHttpServer;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.camel.Exchange;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Runs cursor mode against a local paged HTTP API through Camel's http component. Rows are {@code
 * id,ts}; the API returns rows with {@code ts >= since}, two per page, and names the next page in
 * an {@code X-Next-Page} header.
 */
class CamelCursorSourceIT {

  private final List<long[]> rows = new CopyOnWriteArrayList<>();
  private volatile boolean sorted = true;
  private volatile long delayMs;
  private final AtomicInteger failNext = new AtomicInteger();
  private TestHttpServer api;

  @AfterEach
  void stopApi() {
    if (api != null) {
      api.close();
    }
  }

  @Test
  void followsPagesAndRequestsAgainUntilCaughtUp() throws Exception {
    addRows(1, 10, 2, 10, 3, 10, 4, 20, 5, 30);
    startApi();

    List<Polled> polled = run(builder().bounded(true));

    assertEquals(Set.of("1", "2", "3", "4", "5"), ids(polled));
    assertEquals(1, timestamps(polled).size(), "one poll, one timestamp: " + polled);
    // The last page moved the cursor to 30, so the source asked again from there, got row 5
    // back without progress, and stopped.
    assertEquals(List.of("since=0", "since=0&page=1", "since=0&page=2", "since=30"), queries());
  }

  @Test
  void converterSeesThePollTimestamp() throws Exception {
    addRows(1, 10, 2, 20, 3, 30);
    startApi();

    List<Polled> polled =
        run(
            CamelCursorSource.builder(
                    new RowsCursor(api.url("/rows")),
                    (value, context) -> String.valueOf(context.ingestedAt()),
                    Types.STRING)
                .fetchTimeout(Duration.ofMillis(100))
                .bounded(true));

    // The request after the last page re-reads the boundary row 3.
    assertEquals(4, polled.size(), "" + polled);
    for (Polled record : polled) {
      assertEquals(String.valueOf(record.timestamp()), record.value());
    }
  }

  @Test
  void responseBodyCanBeReadByBothRecordsAndNext() throws Exception {
    addRows(1, 10, 2, 10, 3, 10);
    startApi();

    List<Polled> polled =
        run(
            CamelCursorSource.builder(new BodyReadingCursor(api.url("/rows")))
                .fetchTimeout(Duration.ofMillis(100))
                .bounded(true));

    assertEquals(Set.of("1", "2", "3"), ids(polled));
  }

  @Test
  void watermarksFollowEachCompletedPoll() throws Exception {
    addRows(1, 10, 2, 20);
    startApi();
    rowsAddedLater(3, 30);

    List<Polled> polled =
        run(
            builder().pollInterval(Duration.ofMillis(100)).pollWatermarks(true),
            seen -> ids(seen).contains("3"));

    long firstPoll = polled.get(0).timestamp();
    long previousT = Long.MIN_VALUE;
    Set<Long> completedPolls = new TreeSet<>();
    for (Polled record : polled) {
      assertTrue(record.timestamp() >= previousT, "timestamps go backwards: " + polled);
      if (record.timestamp() > previousT && previousT != Long.MIN_VALUE) {
        completedPolls.add(previousT);
      }
      // Every record precedes its own poll's watermark and follows the previous poll's.
      assertTrue(record.watermark() < record.timestamp(), "late record " + record);
      assertTrue(
          record.timestamp() == firstPoll || completedPolls.contains(record.watermark()),
          "watermark " + record.watermark() + " is not a completed poll: " + polled);
      previousT = record.timestamp();
    }
    assertTrue(completedPolls.size() >= 1, "only one poll: " + polled);
  }

  @Test
  void restoreRerequestsFromTheCheckpointedPositionWithTheSamePollTimestamp() throws Exception {
    for (int i = 1; i <= 10; i++) {
      addRows(i, i);
    }
    delayMs = 100;
    startApi();
    FailOnceAfterCheckpoint.reset();

    StreamExecutionEnvironment env = env(FailOnceAfterCheckpoint.withOneRestart());
    env.enableCheckpointing(20);
    List<Polled> polled =
        PolledRecords.collect(
            env,
            builder().bounded(true).build(),
            stream -> stream.map(new FailOnceAfterCheckpoint<>(4)),
            seen -> false);

    assertTrue(FailOnceAfterCheckpoint.failed());
    assertEquals(
        Arrays.stream(new String[] {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"})
            .collect(Collectors.toSet()),
        ids(polled));
    assertEquals(1, timestamps(polled).size(), "the resumed poll kept its timestamp: " + polled);
    List<String> queries = queries();
    assertEquals(
        1, queries.stream().filter("since=0"::equals).count(), "restarted from 0: " + queries);
    assertTrue(
        queries.stream().anyMatch(q -> q.matches("since=[1-9]")),
        "no request from a checkpointed position: " + queries);
  }

  @Test
  void fullRefreshReadsEverythingOnEveryPoll() throws Exception {
    addRows(1, 10, 2, 20, 3, 30);
    startApi();

    List<Polled> polled =
        run(
            builder().fullRefresh(true).pollInterval(Duration.ofMillis(100)).pollWatermarks(true),
            seen -> seen.size() >= 9);

    Map<Long, Set<String>> byPoll = new TreeMap<>();
    Map<Long, Long> watermarkByPoll = new HashMap<>();
    for (Polled record : polled.subList(0, 9)) {
      byPoll
          .computeIfAbsent(record.timestamp(), t -> new TreeSet<>())
          .add(record.value().split(",")[0]);
      watermarkByPoll.merge(record.watermark(), 1L, Long::sum);
    }
    assertEquals(3, byPoll.size(), "polls: " + byPoll);
    byPoll.values().forEach(ids -> assertEquals(Set.of("1", "2", "3"), ids));
    List<Long> polls = new ArrayList<>(byPoll.keySet());
    for (Polled record : polled.subList(3, 9)) {
      // The previous snapshot is complete before any row of the next one arrives.
      assertEquals(polls.get(polls.indexOf(record.timestamp()) - 1), record.watermark());
    }
    assertTrue(queries().stream().allMatch(q -> q.startsWith("since=0")), "" + queries());
  }

  @Test
  void failsWhenRecordsAreNotInPositionOrder() throws Exception {
    addRows(1, 20, 2, 10);
    sorted = false;
    startApi();

    Exception e = assertThrows(Exception.class, () -> run(builder().bounded(true)));
    assertTrue(
        ExceptionUtils.findThrowableWithMessage(e, "went backwards from 20 to 10").isPresent(),
        ExceptionUtils.stringifyException(e));
  }

  @Test
  void lookbackReadsRowsThatAnotherWriterCommittedLate() throws Exception {
    addRows(1, 10, 3, 30);
    startApi();
    // A slower writer commits row 2 after row 3 was read, so the position is already past it.
    rowsAddedLater(2, 20);

    List<Polled> polled =
        run(
            builder().lookback(Duration.ofMillis(15)).pollInterval(Duration.ofMillis(100)),
            seen -> ids(seen).contains("2"));

    assertEquals(Set.of("1", "2", "3"), ids(polled));
    // Every request starts 15 before the position, and re-reading row 3 doesn't count as
    // progress: no request starts at the position itself.
    assertTrue(queries().stream().noneMatch("since=30"::equals), "" + queries());
  }

  @Test
  void lookbackNeedsAPositionThatCanRewind() {
    Duration window = Duration.ofSeconds(1);
    Instant position = Instant.parse("2026-10-26T05:00:00Z");
    assertEquals(position.minus(window), new Unused<>(position).rewind(position, window));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            CamelCursorSource.builder(new Unused<>(5L), RecordConverter.identity(), Types.STRING)
                .positionType(Types.LONG)
                .lookback(window)
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            CamelCursorSource.builder(new RowsCursor("http://localhost/rows"))
                .fullRefresh(true)
                .lookback(window)
                .build());
  }

  @Test
  void retriesFailedRequestsWithBackoff() throws Exception {
    addRows(1, 10, 2, 20);
    failNext.set(2);
    startApi();

    List<Polled> polled =
        run(builder().bounded(true).retry(3, Duration.ofMillis(10), Duration.ofMillis(20)));

    assertEquals(Set.of("1", "2"), ids(polled));
    assertEquals(0, failNext.get());
  }

  @Test
  void failsTheJobOnARequestErrorWithoutRetries() throws Exception {
    addRows(1, 10);
    failNext.set(1);
    startApi();

    Exception e = assertThrows(Exception.class, () -> run(builder().bounded(true)));
    assertTrue(
        ExceptionUtils.findThrowableWithMessage(e, "Request to http://localhost").isPresent(),
        ExceptionUtils.stringifyException(e));
  }

  @Test
  void splitSerializerRoundTripsTypedPositions() throws Exception {
    TypeSerializer<Instant> instants = Types.INSTANT.createSerializer(new SerializerConfigImpl());
    CursorSplitSerializer<Instant> serializer = new CursorSplitSerializer<>(instants);
    CursorSplit<Instant> split =
        new CursorSplit<>("camel-0", Instant.parse("2026-10-26T05:00:00Z"), 1000L, 2000L);
    assertEquals(split, serializer.deserialize(1, serializer.serialize(split)));

    CursorSplit<Instant> fresh = CursorSplit.initial();
    assertEquals(fresh, serializer.deserialize(1, serializer.serialize(fresh)));

    // A position written as a Long can't be read back as an Instant.
    CursorSplitSerializer<Long> longs =
        new CursorSplitSerializer<>(Types.LONG.createSerializer(new SerializerConfigImpl()));
    byte[] asLong = longs.serialize(new CursorSplit<>("camel-0", 5L, 1L, PollClock.NONE));
    assertThrows(Exception.class, () -> serializer.deserialize(1, asLong));
  }

  // --- fixtures

  /** The API's client: a cursor on {@code ts} with {@code >=}, paging with X-Next-Page. */
  static class RowsCursor implements CursorSource<String, Long, String> {

    private final String url;

    RowsCursor(String url) {
      this.url = url;
    }

    @Override
    public Long initial() {
      return 0L;
    }

    @Override
    public Request request(Long since) {
      return Request.to(url).header(Exchange.HTTP_QUERY, "since=" + since);
    }

    @Override
    public Optional<Request> next(Exchange response) {
      return Optional.ofNullable(response.getMessage().getHeader("X-Next-Page", String.class))
          .map(query -> Request.to(url).header(Exchange.HTTP_QUERY, query));
    }

    @Override
    public Iterable<String> records(Exchange response) {
      String body = response.getMessage().getBody(String.class);
      return body == null || body.isEmpty() ? List.of() : List.of(body.split("\n"));
    }

    @Override
    public String map(String line) {
      return line;
    }

    @Override
    public Long position(String row) {
      return Long.parseLong(row.split(",")[1]);
    }

    /** Positions are milliseconds. */
    @Override
    public Long rewind(Long position, Duration window) {
      return position - window.toMillis();
    }
  }

  /** Reads the response body in {@code next} as well as in {@code records}. */
  static final class BodyReadingCursor extends RowsCursor {

    BodyReadingCursor(String url) {
      super(url);
    }

    @Override
    public Optional<Request> next(Exchange response) {
      String body = response.getMessage().getBody(String.class);
      return body == null || body.isEmpty() ? Optional.empty() : super.next(response);
    }
  }

  /** A cursor that is only built, never run. */
  static final class Unused<POS> implements CursorSource<String, POS, String> {

    private final POS initial;

    Unused(POS initial) {
      this.initial = initial;
    }

    @Override
    public POS initial() {
      return initial;
    }

    @Override
    public Request request(POS position) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String map(String record) {
      return record;
    }

    @Override
    public POS position(String record) {
      throw new UnsupportedOperationException();
    }
  }

  private CamelCursorSource.Builder<String, Long, String, String> builder() {
    return CamelCursorSource.builder(new RowsCursor(api.url("/rows")))
        .fetchTimeout(Duration.ofMillis(100));
  }

  private void startApi() throws Exception {
    api = new TestHttpServer("/rows", request -> respond(request.query()));
  }

  private TestHttpServer.Response respond(String query) {
    sleep(delayMs);
    if (failNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
      return new TestHttpServer.Response(500, "unavailable", Map.of());
    }
    Map<String, String> params = new HashMap<>();
    for (String param : query.split("&")) {
      String[] kv = param.split("=");
      params.put(kv[0], kv[1]);
    }
    long since = Long.parseLong(params.get("since"));
    int page = Integer.parseInt(params.getOrDefault("page", "0"));
    List<long[]> matching = new ArrayList<>();
    for (long[] row : rows) {
      if (row[1] >= since) {
        matching.add(row);
      }
    }
    if (sorted) {
      matching.sort(Comparator.comparingLong((long[] r) -> r[1]).thenComparingLong(r -> r[0]));
    }
    int from = Math.min(page * 2, matching.size());
    int to = Math.min(from + 2, matching.size());
    String body =
        matching.subList(from, to).stream()
            .map(r -> r[0] + "," + r[1])
            .collect(Collectors.joining("\n"));
    Map<String, String> headers =
        to < matching.size()
            ? Map.of("X-Next-Page", "since=" + since + "&page=" + (page + 1))
            : Map.of();
    return new TestHttpServer.Response(200, body, headers);
  }

  private void addRows(long... idTs) {
    for (int i = 0; i < idTs.length; i += 2) {
      rows.add(new long[] {idTs[i], idTs[i + 1]});
    }
  }

  /** Adds the rows once the API has answered a few requests. */
  private void rowsAddedLater(long... idTs) {
    Thread adder =
        new Thread(
            () -> {
              while (api.requests().size() < 3) {
                sleep(10);
              }
              addRows(idTs);
            });
    adder.setDaemon(true);
    adder.start();
  }

  private List<String> queries() {
    return api.requests().stream().map(TestHttpServer.Received::query).toList();
  }

  private List<Polled> run(CamelCursorSource.Builder<String, Long, String, String> builder)
      throws Exception {
    return run(builder, seen -> false);
  }

  private List<Polled> run(
      CamelCursorSource.Builder<String, Long, String, String> builder, Predicate<List<Polled>> done)
      throws Exception {
    return PolledRecords.collect(env(new Configuration()), builder.build(), done);
  }

  private static StreamExecutionEnvironment env(Configuration config) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    return env;
  }

  private static Set<String> ids(List<Polled> polled) {
    return polled.stream().map(p -> p.value().split(",")[0]).collect(Collectors.toSet());
  }

  private static Set<Long> timestamps(List<Polled> polled) {
    return polled.stream().map(Polled::timestamp).collect(Collectors.toSet());
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
