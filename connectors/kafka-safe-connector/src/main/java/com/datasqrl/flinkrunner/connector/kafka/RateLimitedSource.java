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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;
import org.apache.flink.util.Preconditions;

public final class RateLimitedSource<T, SplitT extends SourceSplit, EnumChkT>
    implements Source<T, SplitT, EnumChkT>, LineageVertexProvider {

  private final Source<T, SplitT, EnumChkT> delegate;
  private final RateLimiterStrategy<SplitT> rateLimiterStrategy;

  public RateLimitedSource(
      Source<T, SplitT, EnumChkT> delegate, RateLimiterStrategy<SplitT> rateLimiterStrategy) {
    this.delegate = Preconditions.checkNotNull(delegate, "Delegate source must not be null.");
    this.rateLimiterStrategy =
        Preconditions.checkNotNull(rateLimiterStrategy, "Rate limiter strategy must not be null.");
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
    return new RateLimitedSourceReader<>(
        delegate.createReader(readerContext),
        rateLimiterStrategy.createRateLimiter(readerContext.currentParallelism()));
  }

  @Override
  public SplitEnumerator<SplitT, EnumChkT> createEnumerator(
      SplitEnumeratorContext<SplitT> enumContext) throws Exception {
    return delegate.createEnumerator(enumContext);
  }

  @Override
  public SplitEnumerator<SplitT, EnumChkT> restoreEnumerator(
      SplitEnumeratorContext<SplitT> enumContext, EnumChkT checkpoint) throws Exception {
    return delegate.restoreEnumerator(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<SplitT> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<EnumChkT> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }

  @Override
  public Set<? extends WatermarkDeclaration> declareWatermarks() {
    return delegate.declareWatermarks();
  }

  @Override
  public LineageVertex getLineageVertex() {
    if (delegate instanceof LineageVertexProvider lineageVertexProvider) {
      return lineageVertexProvider.getLineageVertex();
    }
    return new EmptySourceLineageVertex(getBoundedness());
  }

  private record EmptySourceLineageVertex(Boundedness boundedness) implements SourceLineageVertex {

    @Override
    public List<LineageDataset> datasets() {
      return List.of();
    }
  }

  private static final class RateLimitedSourceReader<T, SplitT extends SourceSplit>
      implements SourceReader<T, SplitT> {

    private final SourceReader<T, SplitT> delegate;
    private final RateLimiter<SplitT> rateLimiter;
    private final CountingReaderOutput<T> countingOutput = new CountingReaderOutput<>();
    private CompletableFuture<Void> rateLimitPermissionFuture =
        CompletableFuture.completedFuture(null);

    private RateLimitedSourceReader(
        SourceReader<T, SplitT> delegate, RateLimiter<SplitT> rateLimiter) {
      this.delegate = Preconditions.checkNotNull(delegate, "Delegate reader must not be null.");
      this.rateLimiter = Preconditions.checkNotNull(rateLimiter, "Rate limiter must not be null.");
    }

    @Override
    public void start() {
      delegate.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
      if (!rateLimitPermissionFuture.isDone()) {
        return InputStatus.NOTHING_AVAILABLE;
      }

      countingOutput.reset(output);
      InputStatus status = delegate.pollNext(countingOutput);
      int emittedRecords = countingOutput.getEmittedRecords();
      if (emittedRecords > 0) {
        rateLimitPermissionFuture = rateLimiter.acquire(emittedRecords).toCompletableFuture();
        if (status == InputStatus.MORE_AVAILABLE && !rateLimitPermissionFuture.isDone()) {
          return InputStatus.NOTHING_AVAILABLE;
        }
      }
      return status;
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
      return delegate.snapshotState(checkpointId);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      if (!rateLimitPermissionFuture.isDone()) {
        return rateLimitPermissionFuture;
      }
      return delegate.isAvailable();
    }

    @Override
    public void addSplits(List<SplitT> splits) {
      splits.forEach(rateLimiter::notifyAddingSplit);
      delegate.addSplits(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
      delegate.notifyNoMoreSplits();
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
      delegate.handleSourceEvents(sourceEvent);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      delegate.notifyCheckpointComplete(checkpointId);
      rateLimiter.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void pauseOrResumeSplits(
        Collection<String> splitsToPause, Collection<String> splitsToResume) {
      delegate.pauseOrResumeSplits(splitsToPause, splitsToResume);
    }

    @Override
    public void close() throws Exception {
      delegate.close();
    }
  }

  private static final class CountingReaderOutput<T> implements ReaderOutput<T> {

    private ReaderOutput<T> delegate;
    private final Map<String, CountingSourceOutput<T>> splitOutputs = new HashMap<>();
    private int emittedRecords;

    private void reset(ReaderOutput<T> delegate) {
      ReaderOutput<T> checkedDelegate =
          Preconditions.checkNotNull(delegate, "Delegate output must not be null.");
      if (this.delegate != checkedDelegate) {
        splitOutputs.values().forEach(CountingSourceOutput::resetDelegateOutput);
      }
      this.delegate = checkedDelegate;
      emittedRecords = 0;
    }

    @Override
    public void collect(T record) {
      delegate.collect(record);
      emittedRecords++;
    }

    @Override
    public void collect(T record, long timestamp) {
      delegate.collect(record, timestamp);
      emittedRecords++;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      delegate.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      delegate.markIdle();
    }

    @Override
    public void markActive() {
      delegate.markActive();
    }

    @Override
    public SourceOutput<T> createOutputForSplit(String splitId) {
      return splitOutputs.computeIfAbsent(splitId, id -> new CountingSourceOutput<>(id, this));
    }

    @Override
    public void releaseOutputForSplit(String splitId) {
      splitOutputs.remove(splitId);
      delegate.releaseOutputForSplit(splitId);
    }

    private SourceOutput<T> createDelegateOutputForSplit(String splitId) {
      return delegate.createOutputForSplit(splitId);
    }

    private int getEmittedRecords() {
      return emittedRecords;
    }
  }

  private static final class CountingSourceOutput<T> implements SourceOutput<T> {

    private final String splitId;
    private final CountingReaderOutput<T> readerOutput;
    private SourceOutput<T> delegateOutput;

    private CountingSourceOutput(String splitId, CountingReaderOutput<T> readerOutput) {
      this.splitId = Preconditions.checkNotNull(splitId, "Split ID must not be null.");
      this.readerOutput = readerOutput;
    }

    @Override
    public void collect(T record) {
      delegateOutput().collect(record);
      readerOutput.emittedRecords++;
    }

    @Override
    public void collect(T record, long timestamp) {
      delegateOutput().collect(record, timestamp);
      readerOutput.emittedRecords++;
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      delegateOutput().emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      delegateOutput().markIdle();
    }

    @Override
    public void markActive() {
      delegateOutput().markActive();
    }

    private SourceOutput<T> delegateOutput() {
      if (delegateOutput == null) {
        delegateOutput = readerOutput.createDelegateOutputForSplit(splitId);
      }
      return delegateOutput;
    }

    private void resetDelegateOutput() {
      delegateOutput = null;
    }
  }
}
