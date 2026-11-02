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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiter;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.Preconditions;

/**
 * Rate-limits emitted records while preserving all callbacks of the wrapped source.
 *
 * <p>Flink's built-in rate-limited reader does not forward Kafka's checkpoint-complete callback,
 * which Kafka uses for offset commits.
 */
public final class RateLimitedSource<T, SplitT extends SourceSplit, EnumChkT>
    extends ForwardingSource<T, SplitT, EnumChkT> {

  private final RateLimiterStrategy<SplitT> rateLimiterStrategy;

  public RateLimitedSource(
      Source<T, SplitT, EnumChkT> delegate, RateLimiterStrategy<SplitT> rateLimiterStrategy) {
    super(delegate);
    this.rateLimiterStrategy =
        Preconditions.checkNotNull(rateLimiterStrategy, "Rate limiter strategy must not be null.");
  }

  @Override
  public SourceReader<T, SplitT> createReader(SourceReaderContext readerContext) throws Exception {
    return new RateLimitedSourceReader<>(
        delegate.createReader(readerContext),
        rateLimiterStrategy.createRateLimiter(readerContext.currentParallelism()));
  }

  private static final class RateLimitedSourceReader<T, SplitT extends SourceSplit>
      extends ForwardingReader<T, SplitT> {

    private final RateLimiter<SplitT> rateLimiter;
    private final CountingReaderOutput<T> countingOutput = new CountingReaderOutput<>();
    private CompletableFuture<Void> rateLimitPermissionFuture =
        CompletableFuture.completedFuture(null);

    private RateLimitedSourceReader(
        SourceReader<T, SplitT> delegate, RateLimiter<SplitT> rateLimiter) {
      super(delegate);
      this.rateLimiter = Preconditions.checkNotNull(rateLimiter, "Rate limiter must not be null.");
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
      if (!rateLimitPermissionFuture.isDone()) {
        return InputStatus.NOTHING_AVAILABLE;
      }

      // pollNext() returns only InputStatus, not the emitted-record count. Count actual collect()
      // calls and acquire permits after delegation.
      countingOutput.reset(output);
      InputStatus status = delegate.pollNext(countingOutput);
      int emittedRecords = countingOutput.getEmittedRecords();
      if (emittedRecords > 0) {
        rateLimitPermissionFuture = rateLimiter.acquire(emittedRecords).toCompletableFuture();
        if (status == InputStatus.MORE_AVAILABLE && !rateLimitPermissionFuture.isDone()) {
          // Force the runtime through isAvailable() so it waits on the limiter before polling
          // again.
          return InputStatus.NOTHING_AVAILABLE;
        }
      }
      return status;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      if (!rateLimitPermissionFuture.isDone()) {
        return rateLimitPermissionFuture;
      }
      return super.isAvailable();
    }

    @Override
    public void addSplits(List<SplitT> splits) {
      splits.forEach(rateLimiter::notifyAddingSplit);
      super.addSplits(splits);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      // KafkaSourceReader uses checkpoint completion for offset commits; keep it delegated.
      super.notifyCheckpointComplete(checkpointId);
      rateLimiter.notifyCheckpointComplete(checkpointId);
    }
  }

  // Records can be emitted through either ReaderOutput or split-specific SourceOutput, so both
  // paths need to be counted.
  private static final class CountingReaderOutput<T> implements ReaderOutput<T> {

    private ReaderOutput<T> delegate;
    private final Map<String, CountingSourceOutput<T>> splitOutputs = new HashMap<>();
    private int emittedRecords;

    private void reset(ReaderOutput<T> delegate) {
      ReaderOutput<T> checkedDelegate =
          Preconditions.checkNotNull(delegate, "Delegate output must not be null.");
      if (this.delegate != checkedDelegate) {
        // Split outputs are scoped to the current ReaderOutput. Invalidate cached delegates if
        // Flink supplies a different output instance.
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
