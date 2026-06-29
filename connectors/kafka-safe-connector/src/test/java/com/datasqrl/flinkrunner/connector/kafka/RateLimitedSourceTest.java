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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.junit.jupiter.api.Test;

class RateLimitedSourceTest {

  @Test
  void delegatesSourceMethodsAndCreatesRateLimitedReader() throws Exception {
    FakeSourceReader delegateReader = new FakeSourceReader();
    RateLimitedSource<String, TestSplit, Integer> source =
        new RateLimitedSource<>(new FakeSource(delegateReader), new RecordingStrategy());

    assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    assertThat(source.declareWatermarks()).hasSize(1);
    assertThat(source.declareWatermarks().iterator().next())
        .isSameAs(TestWatermarkDeclaration.INSTANCE);
    assertThat(source.getLineageVertex()).isSameAs(TestLineageVertex.INSTANCE);
  }

  @Test
  void rateLimitsRecordsEmittedThroughSplitOutputs() throws Exception {
    SourceReaderContext readerContext = mock(SourceReaderContext.class);
    when(readerContext.currentParallelism()).thenReturn(2);

    FakeSourceReader delegateReader = new FakeSourceReader();
    RecordingStrategy strategy = new RecordingStrategy();
    SourceReader<String, TestSplit> reader =
        new RateLimitedSource<>(new FakeSource(delegateReader), strategy)
            .createReader(readerContext);
    RecordingReaderOutput output = new RecordingReaderOutput();

    assertThat(strategy.parallelism).isEqualTo(2);

    assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    assertThat(output.records).containsExactly("record-1");
    assertThat(strategy.rateLimiter.acquiredEvents).containsExactly(1);

    assertThat(reader.isAvailable()).isSameAs(strategy.rateLimiter.lastAcquireFuture);
    assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    assertThat(delegateReader.polls).isEqualTo(1);

    strategy.rateLimiter.lastAcquireFuture.complete(null);
    assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
    assertThat(delegateReader.polls).isEqualTo(2);
    assertThat(output.records).containsExactly("record-1", "record-2");
  }

  @Test
  void forwardsSplitAndCheckpointNotifications() throws Exception {
    SourceReaderContext readerContext = mock(SourceReaderContext.class);
    when(readerContext.currentParallelism()).thenReturn(1);

    FakeSourceReader delegateReader = new FakeSourceReader();
    RecordingStrategy strategy = new RecordingStrategy();
    SourceReader<String, TestSplit> reader =
        new RateLimitedSource<>(new FakeSource(delegateReader), strategy)
            .createReader(readerContext);

    TestSplit split = new TestSplit("split-1");
    reader.addSplits(List.of(split));
    reader.notifyCheckpointComplete(42L);

    assertThat(strategy.rateLimiter.addedSplits).containsExactly(split);
    assertThat(strategy.rateLimiter.completedCheckpoints).containsExactly(42L);
    assertThat(delegateReader.splits).containsExactly(split);
    assertThat(delegateReader.completedCheckpoints).containsExactly(42L);
  }

  private record TestSplit(String splitId) implements SourceSplit {}

  private enum TestWatermarkDeclaration implements WatermarkDeclaration {
    INSTANCE;

    @Override
    public String getIdentifier() {
      return "test-watermark";
    }
  }

  private enum TestLineageVertex implements LineageVertex {
    INSTANCE;

    @Override
    public List<LineageDataset> datasets() {
      return List.of();
    }
  }

  private static final class FakeSource
      implements Source<String, TestSplit, Integer>, LineageVertexProvider {

    private final FakeSourceReader reader;

    private FakeSource(FakeSourceReader reader) {
      this.reader = reader;
    }

    @Override
    public Boundedness getBoundedness() {
      return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<String, TestSplit> createReader(SourceReaderContext readerContext) {
      return reader;
    }

    @Override
    public SplitEnumerator<TestSplit, Integer> createEnumerator(
        SplitEnumeratorContext<TestSplit> enumContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SplitEnumerator<TestSplit, Integer> restoreEnumerator(
        SplitEnumeratorContext<TestSplit> enumContext, Integer checkpoint) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimpleVersionedSerializer<TestSplit> getSplitSerializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<? extends WatermarkDeclaration> declareWatermarks() {
      return Set.of(TestWatermarkDeclaration.INSTANCE);
    }

    @Override
    public LineageVertex getLineageVertex() {
      return TestLineageVertex.INSTANCE;
    }
  }

  private static final class FakeSourceReader implements SourceReader<String, TestSplit> {

    private final List<TestSplit> splits = new ArrayList<>();
    private final List<Long> completedCheckpoints = new ArrayList<>();
    private SourceOutput<String> splitOutput;
    private int polls;

    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<String> output) {
      polls++;
      if (splitOutput == null) {
        splitOutput = output.createOutputForSplit("split-1");
      }
      splitOutput.collect("record-" + polls);
      return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<TestSplit> snapshotState(long checkpointId) {
      return splits;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<TestSplit> splits) {
      this.splits.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completedCheckpoints.add(checkpointId);
    }

    @Override
    public void pauseOrResumeSplits(
        Collection<String> splitsToPause, Collection<String> splitsToResume) {}

    @Override
    public void close() {}
  }

  private static final class RecordingStrategy implements RateLimiterStrategy<TestSplit> {

    private final RecordingRateLimiter rateLimiter = new RecordingRateLimiter();
    private int parallelism;

    @Override
    public RateLimiter<TestSplit> createRateLimiter(int parallelism) {
      this.parallelism = parallelism;
      return rateLimiter;
    }
  }

  private static final class RecordingRateLimiter implements RateLimiter<TestSplit> {

    private final List<Integer> acquiredEvents = new ArrayList<>();
    private final List<TestSplit> addedSplits = new ArrayList<>();
    private final List<Long> completedCheckpoints = new ArrayList<>();
    private CompletableFuture<Void> lastAcquireFuture = CompletableFuture.completedFuture(null);

    @Override
    public CompletionStage<Void> acquire(int numberOfEvents) {
      acquiredEvents.add(numberOfEvents);
      lastAcquireFuture = new CompletableFuture<>();
      return lastAcquireFuture;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completedCheckpoints.add(checkpointId);
    }

    @Override
    public void notifyAddingSplit(TestSplit split) {
      addedSplits.add(split);
    }
  }

  private static final class RecordingReaderOutput implements ReaderOutput<String> {

    private final List<String> records = new ArrayList<>();
    private final Map<String, SourceOutput<String>> splitOutputs = new HashMap<>();

    @Override
    public void collect(String record) {
      records.add(record);
    }

    @Override
    public void collect(String record, long timestamp) {
      records.add(record);
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}

    @Override
    public SourceOutput<String> createOutputForSplit(String splitId) {
      return splitOutputs.computeIfAbsent(splitId, ignored -> new RecordingSourceOutput(this));
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
      splitOutputs.remove(splitId);
    }
  }

  private static final class RecordingSourceOutput implements SourceOutput<String> {

    private final RecordingReaderOutput readerOutput;

    private RecordingSourceOutput(RecordingReaderOutput readerOutput) {
      this.readerOutput = readerOutput;
    }

    @Override
    public void collect(String record) {
      readerOutput.collect(record);
    }

    @Override
    public void collect(String record, long timestamp) {
      readerOutput.collect(record, timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}
  }
}
