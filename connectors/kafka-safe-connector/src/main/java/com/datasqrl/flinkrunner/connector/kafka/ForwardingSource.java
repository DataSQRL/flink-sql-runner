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

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.lineage.SourceLineageVertex;

/**
 * A source that forwards every call to the source it wraps, so that a wrapper overrides only what
 * it changes. Flink's own wrappers, such as its rate-limited reader, drop callbacks that a Kafka
 * source relies on, such as the checkpoint completion that commits its offsets.
 */
@Internal
public abstract class ForwardingSource<T, SplitT extends SourceSplit, EnumChkT>
    implements Source<T, SplitT, EnumChkT>, LineageVertexProvider {

  protected final Source<T, SplitT, EnumChkT> delegate;

  protected ForwardingSource(Source<T, SplitT, EnumChkT> delegate) {
    this.delegate = checkNotNull(delegate, "Delegate source must not be null.");
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
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

  /** A reader that forwards every call to the reader it wraps. */
  protected abstract static class ForwardingReader<T, SplitT extends SourceSplit>
      implements SourceReader<T, SplitT> {

    protected final SourceReader<T, SplitT> delegate;

    protected ForwardingReader(SourceReader<T, SplitT> delegate) {
      this.delegate = checkNotNull(delegate, "Delegate reader must not be null.");
    }

    @Override
    public void start() {
      delegate.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
      return delegate.pollNext(output);
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
      return delegate.snapshotState(checkpointId);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return delegate.isAvailable();
    }

    @Override
    public void addSplits(List<SplitT> splits) {
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
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
      delegate.notifyCheckpointAborted(checkpointId);
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
}
