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
package com.datasqrl.flinkrunner.connector.camel.source.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.connector.camel.source.FailOnceAfterCheckpoint;
import com.datasqrl.flinkrunner.connector.camel.source.PolledRecords;
import com.datasqrl.flinkrunner.connector.camel.source.PolledRecords.Polled;
import com.datasqrl.flinkrunner.connector.camel.source.TestHttpServer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.camel.Exchange;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.apache.sshd.common.file.virtualfs.VirtualFileSystemFactory;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.keyprovider.SimpleGeneratorHostKeyProvider;
import org.apache.sshd.sftp.server.SftpSubsystemFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs file mode against local directories, an embedded SFTP server and a local HTTP server. Every
 * line of a file is one record, emitted as {@code file:line}.
 */
class CamelFileSourceIT {

  @TempDir Path dir;

  @Test
  void readsEachNewVersionOnceWithItsOwnTimestampAndLeavesFilesInPlace() throws Exception {
    writeFile("a.txt", "a1", "a2");
    writeFile("b.txt", "b1");
    Thread writer =
        afterRecords(
            3,
            () -> {
              writeFile("c.txt", "c1");
              // Replaced in place: a new version, read again.
              writeFile("a.txt", "a1", "a2", "a3");
              bumpMtime("a.txt");
            });

    List<Polled> polled =
        run(
            builder("file:" + dir + "?readLockCheckInterval=20")
                .pollInterval(Duration.ofMillis(50))
                .pollWatermarks(true),
            writer,
            seen -> values(seen).contains("a.txt:a3") && values(seen).contains("c.txt:c1"));

    // Polls in between saw the same versions and read nothing again.
    assertEquals(List.of("a.txt:a1", "a.txt:a2", "b.txt:b1"), values(polled).subList(0, 3));
    assertEquals(
        List.of("a.txt:a1", "a.txt:a2", "a.txt:a3", "c.txt:c1"),
        values(polled).subList(3, polled.size()).stream().sorted().toList());
    Map<String, Set<Long>> unitsByFile = timestampsByFile(polled);
    // a.txt was read twice, each time as its own unit.
    assertEquals(2, unitsByFile.get("a.txt").size(), "" + unitsByFile);
    assertEquals(1, unitsByFile.get("b.txt").size());
    assertEquals(4, timestamps(polled).size(), "one timestamp per unit: " + polled);
    // Each unit's watermark comes before the next unit's first row.
    long previousT = Long.MIN_VALUE;
    for (Polled record : polled) {
      if (record.timestamp() != previousT && previousT != Long.MIN_VALUE) {
        assertEquals(previousT, record.watermark(), "" + polled);
      }
      previousT = record.timestamp();
    }
    assertEquals(Set.of("a.txt", "b.txt", "c.txt"), filesIn(dir));
  }

  @Test
  void converterSeesTheFileAndRowOfEveryRecord() throws Exception {
    writeFile("a.txt", "x", "skip", "y");

    List<Polled> polled =
        run(
            CamelFileSource.builder(
                    new SkippingLines("file:" + dir),
                    (value, context) ->
                        context.exchange().getMessage().getHeader(Exchange.FILE_NAME, String.class)
                            + ":"
                            + context.row()
                            + ":"
                            + value,
                    Types.STRING)
                .readiness(CamelFileSource.Readiness.none())
                .fetchTimeout(Duration.ofMillis(100))
                .bounded(true),
            null,
            seen -> false);

    assertEquals(List.of("a.txt:1:x", "a.txt:3:y"), values(polled));
  }

  @Test
  void restoreRereadsAHalfEmittedFileFromTheStartWithTheSameTimestamp() throws Exception {
    writeFile("big.txt", IntStream.range(0, 2_000).mapToObj(i -> "row" + i).toArray(String[]::new));
    writeFile("small.txt", "s1");
    FailOnceAfterCheckpoint.reset();

    StreamExecutionEnvironment env = env(FailOnceAfterCheckpoint.withOneRestart());
    env.enableCheckpointing(50);
    List<Polled> polled =
        PolledRecords.collect(
            env,
            builder("file:" + dir)
                .readiness(CamelFileSource.Readiness.none())
                .bounded(true)
                .build(),
            stream -> stream.map(new FailOnceAfterCheckpoint<>(600, 1)),
            seen -> false);

    assertTrue(FailOnceAfterCheckpoint.failed());
    Set<String> expected = new TreeSet<>();
    IntStream.range(0, 2_000).forEach(i -> expected.add("big.txt:row" + i));
    expected.add("small.txt:s1");
    assertEquals(expected, new TreeSet<>(values(polled)));
    Map<String, Set<Long>> unitsByFile = timestampsByFile(polled);
    assertEquals(
        1, unitsByFile.get("big.txt").size(), "re-read with a new timestamp: " + unitsByFile);
    assertTrue(
        unitsByFile.get("small.txt").iterator().next()
            > unitsByFile.get("big.txt").iterator().next());
    assertTrue(polled.size() > expected.size(), "nothing was re-read: " + polled.size());
  }

  @Test
  void listingUnitRereadsEveryFileWhenTheListingChanges() throws Exception {
    writeFile("part-1.csv", "p1");
    writeFile("part-2.csv", "p2");
    Thread writer = afterRecords(2, () -> writeFile("part-3.csv", "p3"));

    List<Polled> polled =
        run(
            builder("file:" + dir)
                .readiness(CamelFileSource.Readiness.none())
                .snapshotUnit(CamelFileSource.SnapshotUnit.LISTING)
                .pollInterval(Duration.ofMillis(50))
                .pollWatermarks(true),
            writer,
            seen -> seen.size() >= 5);

    Map<Long, Set<String>> byUnit = new TreeMap<>();
    for (Polled record : polled) {
      byUnit.computeIfAbsent(record.timestamp(), t -> new TreeSet<>()).add(record.value());
    }
    assertEquals(
        List.of(
            Set.of("part-1.csv:p1", "part-2.csv:p2"),
            Set.of("part-1.csv:p1", "part-2.csv:p2", "part-3.csv:p3")),
        new ArrayList<>(byUnit.values()));
    long firstUnit = byUnit.keySet().iterator().next();
    polled.stream()
        .filter(record -> record.timestamp() != firstUnit)
        .forEach(record -> assertEquals(firstUnit, record.watermark()));
  }

  @Test
  void holdsBackAFileUntilItsTriggerFileExists() throws Exception {
    writeFile("gl.dat", "posting");
    AtomicReference<Long> triggeredAt = new AtomicReference<>();
    Thread trigger =
        new Thread(
            () -> {
              sleep(1_000);
              triggeredAt.set(System.currentTimeMillis());
              writeFile("gl.dat.done");
            });

    List<Polled> polled =
        run(
            builder("file:" + dir)
                .readiness(CamelFileSource.Readiness.trigger(".done"))
                .pollInterval(Duration.ofMillis(50)),
            trigger,
            seen -> !seen.isEmpty());

    assertEquals(List.of("gl.dat:posting"), values(polled));
    assertTrue(polled.get(0).timestamp() >= triggeredAt.get(), "read before its trigger file");
  }

  @Test
  void failsWhenAUnitShrinksBelowTheMinRowRatio() throws Exception {
    writeFile("2026-10-01.csv", "r1", "r2", "r3", "r4", "r5");
    writeFile("2026-10-02.csv", "r1");

    Exception e =
        assertThrows(
            Exception.class,
            () ->
                run(
                    builder("file:" + dir)
                        .readiness(CamelFileSource.Readiness.none())
                        .minRowRatio(0.5)
                        .bounded(true),
                    null,
                    seen -> false));
    assertTrue(
        ExceptionUtils.findThrowableWithMessage(
                e, "has 1 rows, fewer than 0.5 of the previous unit's 5")
            .isPresent(),
        ExceptionUtils.stringifyException(e));
  }

  @Test
  void rejectsEndpointsThatWouldModifyTheLocation() {
    Exception e =
        assertThrows(
            Exception.class,
            () -> run(builder("file:" + dir + "?delete=true").bounded(true), null, seen -> false));
    assertTrue(
        ExceptionUtils.findThrowableWithMessage(e, "never modifies the source location")
            .isPresent(),
        ExceptionUtils.stringifyException(e));
  }

  /** In ack mode, SFTP delivered a file waiting for its ack again on every poll. */
  @Test
  void sftpReadsEachFileOnce() throws Exception {
    Path root = Files.createDirectories(dir.resolve("sftp"));
    Path inbox = Files.createDirectories(root.resolve("inbox"));
    SshServer sftp = SshServer.setUpDefaultServer();
    sftp.setHost("localhost");
    sftp.setPort(0);
    sftp.setKeyPairProvider(new SimpleGeneratorHostKeyProvider(dir.resolve("hostkey.ser")));
    sftp.setPasswordAuthenticator((user, password, session) -> "secret".equals(password));
    sftp.setSubsystemFactories(List.of(new SftpSubsystemFactory()));
    sftp.setFileSystemFactory(new VirtualFileSystemFactory(root));
    sftp.start();
    try {
      Files.writeString(inbox.resolve("a.txt"), "a1\na2\n");
      Files.writeString(inbox.resolve("b.txt"), "b1\n");
      Thread writer =
          afterRecords(
              3,
              () -> {
                // Several polls go by before the last file arrives.
                sleep(500);
                writeQuietly(inbox.resolve("z.txt"), "z1\n");
              });
      String uri =
          "sftp://tester@localhost:"
              + sftp.getPort()
              + "/inbox?password=secret"
              + "&strictHostKeyChecking=no&useUserKnownHostsFile=false"
              + "&readLockCheckInterval=50";

      List<Polled> polled =
          run(
              builder(uri).pollInterval(Duration.ofMillis(50)),
              writer,
              seen -> values(seen).contains("z.txt:z1"));

      assertEquals(List.of("a.txt:a1", "a.txt:a2", "b.txt:b1", "z.txt:z1"), values(polled));
      assertEquals(Set.of("a.txt", "b.txt", "z.txt"), filesIn(inbox));
    } finally {
      sftp.stop();
    }
  }

  @Test
  void urlIsReadAgainOnlyWhenItsContentChanges() throws Exception {
    AtomicReference<String> content = new AtomicReference<>("v1-a\nv1-b\n");
    AtomicReference<String> etag = new AtomicReference<>("\"1\"");
    AtomicInteger notModified = new AtomicInteger();
    try (TestHttpServer server =
        new TestHttpServer(
            "/sdn.csv",
            request -> {
              if (etag.get().equals(request.headers().getFirst("If-None-Match"))) {
                notModified.incrementAndGet();
                return new TestHttpServer.Response(304, "", Map.of("ETag", etag.get()));
              }
              return new TestHttpServer.Response(200, content.get(), Map.of("ETag", etag.get()));
            })) {
      Thread publisher =
          afterRecords(
              2,
              () -> {
                while (notModified.get() < 2) {
                  sleep(10);
                }
                // Republished unchanged: a new ETag but the same content.
                etag.set("\"2\"");
                while (notModified.get() < 4) {
                  sleep(10);
                }
                content.set("v2-a\n");
                etag.set("\"3\"");
              });

      List<Polled> polled =
          run(
              builder(server.url("/sdn.csv")).pollInterval(Duration.ofMillis(50)),
              publisher,
              seen -> values(seen).contains("/sdn.csv:v2-a"));

      assertEquals(List.of("/sdn.csv:v1-a", "/sdn.csv:v1-b", "/sdn.csv:v2-a"), values(polled));
      assertTrue(notModified.get() >= 4);
    }
  }

  @Test
  void splitStateEvictsVersionsNoListingHasContainedForTheRetention() throws Exception {
    FileSplitState state = new FileSplitState(FileSplit.initial());
    state.unitStarted(1_000, "a@1@1");
    state.fileCompleted(1_000, "a@1@1", 3, 10_000);
    state.unitStarted(1_001, "b@1@1");
    state.fileCompleted(1_001, "b@1@1", 3, 10_000);
    state.unitStarted(1_002, "c@1@1");

    // b is still listed; a isn't, and goes once the retention has passed.
    state.listed(Set.of("b@1@1"), 15_000, 10_000);
    assertEquals(Set.of("a@1@1", "b@1@1"), state.toSplit().seen().keySet());
    state.listed(Set.of("b@1@1"), 21_000, 10_000);
    assertEquals(Set.of("b@1@1"), state.toSplit().seen().keySet());

    FileSplit split = state.toSplit();
    assertEquals(1_002, split.unitT());
    assertEquals("c@1@1", split.unitKey());
    assertEquals(
        split,
        FileSplitSerializer.INSTANCE.deserialize(1, FileSplitSerializer.INSTANCE.serialize(split)));
  }

  // --- fixtures

  /** Every line of a file is a record, emitted as {@code name:line}. */
  static final class Lines implements FileSource<String, String> {

    private final String location;

    Lines(String location) {
      this.location = location;
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public Iterable<String> records(Exchange file) {
      // A URL's name is the URL; keep only its path.
      String name = file.getMessage().getHeader(Exchange.FILE_NAME, String.class);
      String prefix = name.replaceFirst("^https?://[^/]+", "") + ":";
      return file.getMessage().getBody(String.class).lines().map(line -> prefix + line).toList();
    }

    @Override
    public String map(String line) {
      return line;
    }
  }

  /** Every line of a file is a record, emitted as is; the line {@code skip} is dropped. */
  static final class SkippingLines implements FileSource<String, String> {

    private final String location;

    SkippingLines(String location) {
      this.location = location;
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public Iterable<String> records(Exchange file) {
      return file.getMessage().getBody(String.class).lines().toList();
    }

    @Override
    public String map(String line) {
      return "skip".equals(line) ? null : line;
    }
  }

  private static CamelFileSource.Builder<String, String, String> builder(String location) {
    return CamelFileSource.builder(new Lines(location)).fetchTimeout(Duration.ofMillis(100));
  }

  private static List<Polled> run(
      CamelFileSource.Builder<String, String, String> builder,
      Thread sideEffect,
      Predicate<List<Polled>> done)
      throws Exception {
    PolledRecords.resetCollected();
    if (sideEffect != null) {
      sideEffect.setDaemon(true);
      sideEffect.start();
    }
    return PolledRecords.collect(env(new Configuration()), builder.build(), done);
  }

  /**
   * Runs {@code action} once the test has collected {@code count} records. Not started until the
   * job runs.
   */
  private static Thread afterRecords(int count, Runnable action) {
    return new Thread(
        () -> {
          long deadline = System.currentTimeMillis() + 60_000;
          while (PolledRecords.collectedSoFar() < count && System.currentTimeMillis() < deadline) {
            sleep(20);
          }
          action.run();
        });
  }

  private void writeFile(String name, String... lines) {
    writeQuietly(dir.resolve(name), lines.length == 0 ? "" : String.join("\n", lines) + "\n");
  }

  private void bumpMtime(String name) {
    try {
      Path path = dir.resolve(name);
      Files.setLastModifiedTime(
          path, FileTime.fromMillis(Files.getLastModifiedTime(path).toMillis() + 2_000));
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void writeQuietly(Path path, String content) {
    try {
      Files.writeString(path, content);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Set<String> filesIn(Path directory) throws IOException {
    try (Stream<Path> files = Files.list(directory)) {
      return files
          .filter(Files::isRegularFile)
          .map(p -> p.getFileName().toString())
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  private static StreamExecutionEnvironment env(Configuration config) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
    env.setParallelism(1);
    return env;
  }

  private static List<String> values(List<Polled> polled) {
    return polled.stream().map(Polled::value).toList();
  }

  private static Set<Long> timestamps(List<Polled> polled) {
    return polled.stream().map(Polled::timestamp).collect(Collectors.toSet());
  }

  private static Map<String, Set<Long>> timestampsByFile(List<Polled> polled) {
    Map<String, Set<Long>> byFile = new HashMap<>();
    for (Polled record : polled) {
      byFile
          .computeIfAbsent(record.value().split(":")[0], f -> new TreeSet<>())
          .add(record.timestamp());
    }
    return byFile;
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
