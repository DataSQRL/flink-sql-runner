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
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.Bind;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.camel.Component;
import org.apache.camel.Exchange;
import org.apache.camel.component.file.GenericFile;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the source against Camel's file component, which deletes a file only when its exchange is
 * acknowledged. A file still on disk after a run means Camel was never told to commit it.
 */
class CamelAckSourceIT {

  private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
  private static final int MAX_PENDING = 5;
  private static final Duration FETCH_AHEAD_GRACE = Duration.ofMillis(500);

  @TempDir Path inbox;

  @Test
  void redeliversUntilACheckpointCommits() throws Exception {
    writeFiles("a", "b", "c", "d", "e");
    Set<String> expected = Set.of("record-a", "record-b", "record-c", "record-d", "record-e");
    // Without checkpoints nothing is safe to acknowledge, so every file stays for redelivery.
    assertEquals(expected, run(bodies(body -> body), false));
    assertEquals(Set.of("a.txt", "b.txt", "c.txt", "d.txt", "e.txt"), filesInInbox());

    // The same files are delivered again, and the final checkpoint acknowledges them.
    assertEquals(expected, run(bodies(body -> body), true));
    awaitEmptyInbox();
  }

  @Test
  void acknowledgesDroppedAndFailedRecords() throws Exception {
    writeFiles("keep", "drop", "fail");
    BodyAckSource.Mapping mapping =
        body -> {
          if (body.equals("record-drop")) {
            return null;
          }
          if (body.equals("record-fail")) {
            throw new IllegalStateException("cannot map " + body);
          }
          return body;
        };

    assertEquals(Set.of("record-keep"), run(bodies(mapping), true));
    awaitEmptyInbox();
  }

  @Test
  void converterSeesTheReceivedExchange() throws Exception {
    writeFile("a.json", "{}");
    writeFile("b.json", "{}");

    Set<String> records =
        run(
            converted(
                (value, context) ->
                    context
                        .exchange()
                        .getMessage()
                        .getHeader(Exchange.FILE_NAME_ONLY, String.class)),
            true);

    assertEquals(Set.of("a.json", "b.json"), records);
  }

  @Test
  void acknowledgesRecordsTheConverterFailsOn() throws Exception {
    writeFiles("a", "b");

    Set<String> records =
        run(
            converted(
                (value, context) -> {
                  throw new IllegalStateException("cannot convert " + value);
                }),
            true);

    assertEquals(Set.of(), records);
    awaitEmptyInbox();
  }

  @Test
  void failoverRedeliversRecordsNoCheckpointCovered() throws Exception {
    writeFiles("a", "b", "c");
    FAILED_ONCE.set(false);

    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);

    // The map is chained to the source, so the first record fails the job before any
    // checkpoint can cover it. The restarted reader must receive it again.
    Set<String> records =
        run(
            bodies(body -> body),
            true,
            config,
            stream ->
                stream.map(
                    value -> {
                      if (!FAILED_ONCE.getAndSet(true)) {
                        throw new IllegalStateException("boom");
                      }
                      return value;
                    }));

    assertTrue(FAILED_ONCE.get());
    assertEquals(Set.of("record-a", "record-b", "record-c"), records);
    awaitEmptyInbox();
  }

  @Test
  void bindMethodsAreRegisteredBeforeTheEndpointIsCreated() throws Exception {
    writeFiles("a", "b", "c");

    List<String> records =
        runInOrder(
            configured(
                CamelAckSource.builder(
                    new ReverseSortedInbox(
                        "file:" + inbox + "?delete=true&delay=10&sorter=#reverse"))));

    assertEquals(List.of("record-c", "record-b", "record-a"), records);
  }

  @Test
  void componentOptionsConfigureTheComponent() throws Exception {
    CamelAckSource<String> source =
        configured(
            CamelAckSource.builder(new TimerCounter())
                .componentOptions(Map.of("timer.includeMetadata", "true")));

    assertEquals(List.of("1"), runInOrder(source));
  }

  @Test
  void unknownComponentOptionFailsTheJob() {
    CamelAckSource<String> source =
        configured(
            CamelAckSource.builder(new TimerCounter()).componentOptions(Map.of("timer.nope", "1")));

    Exception failure = assertThrows(Exception.class, () -> run(source, false));

    Throwable root = failure;
    while (root.getCause() != null) {
      root = root.getCause();
    }
    assertTrue(root.getMessage().contains("nope"), root.toString());
  }

  @Test
  void maxPendingBoundsWhatCamelFetchesAhead() throws Exception {
    PushingComponent.PUSHED.set(0);
    CamelAckSource<String> source =
        CamelAckSource.builder(new Pushed())
            .receiveTimeout(Duration.ofMillis(200))
            .maxPending(MAX_PENDING)
            .build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // Without checkpoints nothing is acknowledged, so the source stops receiving at MAX_PENDING.
    try (CloseableIterator<String> results =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "camel").executeAndCollect()) {
      for (int i = 0; i < MAX_PENDING; i++) {
        results.next();
      }
      Thread.sleep(FETCH_AHEAD_GRACE.toMillis());

      // Besides the received exchanges, one waits in Camel's queue and one in the component's
      // hands.
      int pushed = PushingComponent.PUSHED.get();
      assertTrue(pushed <= MAX_PENDING + 2, "the component pushed " + pushed + " exchanges");
    }
  }

  /** Receives from a {@link PushingComponent} it binds. */
  public static final class Pushed implements AckSource<String> {

    @Bind("pushing")
    public Component pushing() {
      return new PushingComponent();
    }

    @Override
    public String endpoint() {
      return "pushing:numbers";
    }

    @Override
    public String map(Exchange exchange) {
      return exchange.getMessage().getBody(String.class);
    }
  }

  /** Reads an inbox whose endpoint sorts files with a comparator the source binds. */
  public record ReverseSortedInbox(String endpoint) implements AckSource<String> {

    @Bind("reverse")
    public Comparator<GenericFile<File>> reverse() {
      return Comparator.comparing((GenericFile<File> file) -> file.getFileName()).reversed();
    }

    @Override
    public String map(Exchange exchange) {
      return exchange.getMessage().getBody(String.class);
    }
  }

  /** Fires once, emitting the timer's counter, which the timer only sets with its metadata on. */
  public static final class TimerCounter implements AckSource<String> {

    @Override
    public String endpoint() {
      return "timer:tick?repeatCount=1&delay=0";
    }

    @Override
    public String map(Exchange exchange) {
      return String.valueOf(exchange.getProperty(Exchange.TIMER_COUNTER));
    }
  }

  private CamelAckSource<String> bodies(BodyAckSource.Mapping mapping) {
    return configured(CamelAckSource.builder(new BodyAckSource(inboxUri(), mapping)));
  }

  private CamelAckSource<String> converted(RecordConverter<String, String> converter) {
    return configured(
        CamelAckSource.builder(
            new BodyAckSource(inboxUri(), body -> body), converter, Types.STRING));
  }

  private static CamelAckSource<String> configured(CamelAckSource.Builder<String, String> builder) {
    return builder.receiveTimeout(Duration.ofMillis(200)).stopAfterEmptyPolls(3).build();
  }

  private String inboxUri() {
    return "file:" + inbox + "?delete=true&delay=10";
  }

  private Set<String> run(CamelAckSource<String> source, boolean checkpointing) throws Exception {
    return run(source, checkpointing, new Configuration(), stream -> stream);
  }

  private List<String> runInOrder(CamelAckSource<String> source) throws Exception {
    return collect(source, true, new Configuration(), stream -> stream);
  }

  private Set<String> run(
      CamelAckSource<String> source,
      boolean checkpointing,
      Configuration config,
      UnaryOperator<DataStream<String>> pipeline)
      throws Exception {
    List<String> records = collect(source, checkpointing, config, pipeline);
    assertEquals(records.size(), Set.copyOf(records).size(), "duplicates in " + records);
    return Set.copyOf(records);
  }

  private List<String> collect(
      CamelAckSource<String> source,
      boolean checkpointing,
      Configuration config,
      UnaryOperator<DataStream<String>> pipeline)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    if (checkpointing) {
      env.enableCheckpointing(50);
    }

    try (CloseableIterator<String> results =
        pipeline
            .apply(env.fromSource(source, WatermarkStrategy.noWatermarks(), "camel"))
            .executeAndCollect()) {
      return CollectionUtil.iteratorToList(results);
    }
  }

  private void writeFiles(String... names) throws IOException {
    for (String name : names) {
      writeFile(name + ".txt", "record-" + name);
    }
  }

  private void writeFile(String name, String content) throws IOException {
    Files.writeString(inbox.resolve(name), content);
  }

  private Set<String> filesInInbox() throws IOException {
    try (Stream<Path> files = Files.list(inbox)) {
      return files
          .filter(Files::isRegularFile)
          .map(p -> p.getFileName().toString())
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  private void awaitEmptyInbox() throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (!filesInInbox().isEmpty()) {
      if (System.nanoTime() > deadline) {
        fail("Files were never acknowledged: " + filesInInbox());
      }
      Thread.sleep(50);
    }
  }
}
