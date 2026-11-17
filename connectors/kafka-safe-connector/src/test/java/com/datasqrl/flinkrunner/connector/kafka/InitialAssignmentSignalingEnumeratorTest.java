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
package com.datasqrl.flinkrunner.connector.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link InitialAssignmentSignalingEnumerator} around the Kafka connector's own enumerator,
 * whose async calls it follows. The partitions come from a subscriber, so no broker is needed.
 */
class InitialAssignmentSignalingEnumeratorTest {

  private static final int PARALLELISM = 3;
  private static final TopicPartition PARTITION_0 = new TopicPartition("topic", 0);
  private static final TopicPartition PARTITION_1 = new TopicPartition("topic", 1);

  private final FixedSubscriber subscriber = new FixedSubscriber(PARTITION_0, PARTITION_1);
  private final Coordinator coordinator = new Coordinator();
  private SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> enumerator;

  @AfterEach
  void closeEnumerator() throws Exception {
    if (enumerator != null) {
      enumerator.close();
    }
  }

  @Test
  void testSignalsReadersOnceSplitsOfFirstDiscoveryAreAssigned() throws Exception {
    enumerator = createEnumerator(false);
    registerReaders();
    enumerator.start();

    assertThat(coordinator.sent).isEmpty();

    // Lists the partitions, and asks where their splits start in a second call.
    coordinator.runNextAsyncCall();

    assertThat(coordinator.sent).isEmpty();

    coordinator.runNextAsyncCall();

    // Two partitions for three readers: one gets no split, and still the event.
    assertThat(coordinator.sent).filteredOn(sent -> sent.startsWith("splits")).hasSize(2);
    assertThat(coordinator.sent.subList(2, 5))
        .containsExactly("done to 0", "done to 1", "done to 2");
    assertThat(coordinator.asyncCalls).isEmpty();
  }

  @Test
  void testSignalsReaderRegisteringLaterAfterItsPendingSplits() throws Exception {
    enumerator = createEnumerator(false);
    enumerator.start();
    coordinator.runAsyncCalls();

    assertThat(coordinator.sent).isEmpty();

    registerReaders();

    assertThat(coordinator.sent).filteredOn(sent -> sent.startsWith("done")).hasSize(3);
    for (int reader = 0; reader < PARALLELISM; reader++) {
      int splits = coordinator.sent.indexOf("splits to " + reader);
      assertThat(splits).isLessThan(coordinator.sent.indexOf("done to " + reader));
    }
  }

  @Test
  void testSignalsRestoredReadersWhenDiscoveryFindsNothingNew() throws Exception {
    // The restored readers hold every partition, so the enumerator makes no second call.
    enumerator = restoreEnumerator(true);
    registerReaders();
    enumerator.start();

    assertThat(coordinator.sent).isEmpty();

    coordinator.runNextAsyncCall();

    assertThat(coordinator.sent).containsExactly("done to 0", "done to 1", "done to 2");
    assertThat(coordinator.asyncCalls).isEmpty();
  }

  @Test
  void testSignalsRestoredReadersWhenEnumeratorKeepsItsDiscoveryUnfinished() throws Exception {
    // Restored from a checkpoint taken before its first discovery finished, the enumerator finds
    // nothing new and leaves its own flag unset, which therefore cannot stand in for the event.
    enumerator = restoreEnumerator(false);
    registerReaders();
    enumerator.start();
    coordinator.runAsyncCalls();

    assertThat(coordinator.sent).containsExactly("done to 0", "done to 1", "done to 2");
    assertThat(enumerator.snapshotState(1L).initialDiscoveryFinished()).isFalse();
  }

  @Test
  void testSignalsOnceWithPeriodicDiscovery() throws Exception {
    enumerator = createEnumerator(true);
    registerReaders();
    enumerator.start();
    coordinator.runAsyncCalls();
    int sentByFirstDiscovery = coordinator.sent.size();

    subscriber.partitions.add(new TopicPartition("topic", 2));
    coordinator.runPeriodicAsyncCalls();

    assertThat(coordinator.sent.subList(sentByFirstDiscovery, coordinator.sent.size()))
        .singleElement()
        .asString()
        .startsWith("splits to ");
  }

  @Test
  void testDoesNotSignalWhenDiscoveryFails() throws Exception {
    enumerator = createEnumerator(false);
    registerReaders();
    subscriber.failing = true;
    enumerator.start();

    assertThatThrownBy(coordinator::runAsyncCalls)
        .hasMessageContaining("Failed to list subscribed topic partitions");
    assertThat(coordinator.sent).isEmpty();
  }

  @Test
  @SuppressWarnings("unchecked")
  void testForwardsEnumeratorCallbacks() throws Exception {
    SplitEnumerator<KafkaPartitionSplit, String> delegate = mock(SplitEnumerator.class);
    var signaling = new InitialAssignmentSignalingEnumerator<>(coordinator, context -> delegate);
    SourceEvent event = mock(SourceEvent.class);
    var splits = List.of(new KafkaPartitionSplit(PARTITION_0, 5L));

    signaling.handleSplitRequest(1, "host");
    signaling.addSplitsBack(splits, 1);
    signaling.snapshotState(7L);
    signaling.notifyCheckpointComplete(7L);
    signaling.notifyCheckpointAborted(8L);
    signaling.handleSourceEvent(1, event);
    signaling.close();

    verify(delegate).handleSplitRequest(1, "host");
    verify(delegate).addSplitsBack(splits, 1);
    verify(delegate).snapshotState(7L);
    verify(delegate).notifyCheckpointComplete(7L);
    verify(delegate).notifyCheckpointAborted(8L);
    verify(delegate).handleSourceEvent(1, event);
    verify(delegate).close();
  }

  private SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
      boolean periodicDiscovery) throws Exception {
    var source = kafkaSource(periodicDiscovery);
    return new InitialAssignmentSignalingEnumerator<>(coordinator, source::createEnumerator);
  }

  /** An enumerator restored with both partitions assigned. */
  private SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
      boolean initialDiscoveryFinished) throws Exception {
    var source = kafkaSource(false);
    var checkpoint =
        new KafkaSourceEnumState(
            List.of(
                new KafkaPartitionSplit(PARTITION_0, 5L), new KafkaPartitionSplit(PARTITION_1, 7L)),
            List.of(),
            initialDiscoveryFinished);
    return new InitialAssignmentSignalingEnumerator<>(
        coordinator, context -> source.restoreEnumerator(context, checkpoint));
  }

  private KafkaSource<String> kafkaSource(boolean periodicDiscovery) {
    return KafkaSource.<String>builder()
        .setBootstrapServers("localhost:1")
        .setKafkaSubscriber(subscriber)
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setProperty("partition.discovery.interval.ms", periodicDiscovery ? "1000" : "-1")
        .build();
  }

  private void registerReaders() {
    for (int reader = 0; reader < PARALLELISM; reader++) {
      coordinator.readers.put(reader, new ReaderInfo(reader, "host"));
      enumerator.addReader(reader);
    }
  }

  /** The partitions of the topic, without asking a broker. */
  private static final class FixedSubscriber implements KafkaSubscriber {

    private final Set<TopicPartition> partitions = new HashSet<>();
    private boolean failing;

    private FixedSubscriber(TopicPartition... partitions) {
      this.partitions.addAll(List.of(partitions));
    }

    @Override
    public Set<TopicPartition> getSubscribedTopicPartitions(AdminClient adminClient) {
      if (failing) {
        throw new IllegalStateException("Broker cannot be reached");
      }
      return new HashSet<>(partitions);
    }
  }

  /**
   * The source coordinator's context: it runs the enumerator's async calls, each with its handler,
   * when the test says so, and records what the enumerator sends to which reader.
   */
  private static final class Coordinator implements SplitEnumeratorContext<KafkaPartitionSplit> {

    private final Map<Integer, ReaderInfo> readers = new TreeMap<>();
    private final List<String> sent = new ArrayList<>();
    private final Deque<Runnable> asyncCalls = new ArrayDeque<>();
    private final List<Runnable> periodicAsyncCalls = new ArrayList<>();

    private void runNextAsyncCall() {
      asyncCalls.remove().run();
    }

    private void runAsyncCalls() {
      while (!asyncCalls.isEmpty()) {
        runNextAsyncCall();
      }
    }

    /** The next run of every periodic call, and what its handler calls in turn. */
    private void runPeriodicAsyncCalls() {
      asyncCalls.addAll(periodicAsyncCalls);
      runAsyncCalls();
    }

    private <T> Runnable asyncCall(Callable<T> callable, BiConsumer<T, Throwable> handler) {
      return () -> {
        T result = null;
        Throwable error = null;
        try {
          result = callable.call();
        } catch (Throwable t) {
          error = t;
        }
        handler.accept(result, error);
      };
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
      asyncCalls.add(asyncCall(callable, handler));
    }

    @Override
    public <T> void callAsync(
        Callable<T> callable,
        BiConsumer<T, Throwable> handler,
        long initialDelayMillis,
        long periodMillis) {
      Runnable asyncCall = asyncCall(callable, handler);
      asyncCalls.add(asyncCall);
      periodicAsyncCalls.add(asyncCall);
    }

    @Override
    public void assignSplits(SplitsAssignment<KafkaPartitionSplit> newSplitAssignments) {
      new TreeMap<>(newSplitAssignments.assignment())
          .forEach((reader, splits) -> sent.add("splits to " + reader));
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
      assertThat(event).isInstanceOf(InitialAssignmentDoneEvent.class);
      sent.add("done to " + subtaskId);
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
      return readers;
    }

    @Override
    public int currentParallelism() {
      return PARALLELISM;
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
      return null;
    }

    @Override
    public void signalNoMoreSplits(int subtask) {}

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
      runnable.run();
    }
  }
}
