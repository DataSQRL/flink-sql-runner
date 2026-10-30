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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.util.Preconditions;

/**
 * Records in {@link KafkaReadPositions} every split assigned to the reader, for the idle
 * advancement of {@link KafkaRecordTimestampWatermarkStrategy}. Flink calls {@code addSplits} on
 * the source task's thread, the thread whose positions the strategy reads.
 */
@Internal
public final class AssignmentTrackingSource<T>
    implements Source<T, KafkaPartitionSplit, KafkaSourceEnumState>,
        LineageVertexProvider,
        ResultTypeQueryable<T> {

  private final KafkaSource<T> delegate;

  public AssignmentTrackingSource(KafkaSource<T> delegate) {
    this.delegate = Preconditions.checkNotNull(delegate, "Delegate source must not be null.");
  }

  @Override
  public Boundedness getBoundedness() {
    return delegate.getBoundedness();
  }

  @Override
  public SourceReader<T, KafkaPartitionSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return new AssignmentTrackingReader<>(delegate.createReader(readerContext));
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext) throws Exception {
    return delegate.createEnumerator(enumContext);
  }

  @Override
  public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> enumContext, KafkaSourceEnumState checkpoint)
      throws Exception {
    return delegate.restoreEnumerator(enumContext, checkpoint);
  }

  @Override
  public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() {
    return delegate.getSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() {
    return delegate.getEnumeratorCheckpointSerializer();
  }

  @Override
  public Set<? extends WatermarkDeclaration> declareWatermarks() {
    return delegate.declareWatermarks();
  }

  @Override
  public LineageVertex getLineageVertex() {
    return delegate.getLineageVertex();
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return delegate.getProducedType();
  }

  record AssignmentTrackingReader<T>(SourceReader<T, KafkaPartitionSplit> delegate)
      implements SourceReader<T, KafkaPartitionSplit> {

    @Override
    public void start() {
      delegate.start();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
      return delegate.pollNext(output);
    }

    @Override
    public List<KafkaPartitionSplit> snapshotState(long checkpointId) {
      return delegate.snapshotState(checkpointId);
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
      return delegate.isAvailable();
    }

    @Override
    public void addSplits(List<KafkaPartitionSplit> splits) {
      splits.forEach(
          split ->
              KafkaReadPositions.assigned(
                  split.getTopic(), split.getPartition(), split.getStartingOffset()));
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
      // KafkaSourceReader commits offsets on checkpoint completion.
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
