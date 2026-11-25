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

import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.camel.Exchange;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

/**
 * Ack-mode source reader. An exchange is acknowledged only after a checkpoint that covers its
 * emission completes, so Camel never commits a message Flink could still lose.
 *
 * <ul>
 *   <li>The emitter adds every exchange to the current buffer.
 *   <li>{@link #snapshotState(long)} moves the current buffer under the checkpoint id.
 *   <li>{@link #notifyCheckpointComplete(long)} acknowledges every buffer at or below the id.
 *       Aborted checkpoints stay pending until a later one completes.
 *   <li>Nothing about the exchanges is restored. Unacknowledged messages are redelivered by the
 *       source system.
 * </ul>
 */
final class AckSourceReader<OUT>
    extends SingleThreadMultiplexSourceReaderBase<AckRecord<OUT>, OUT, AckSplit, AckSplit> {

  private final CamelRuntime runtime;
  private final PendingAcks pendingAcks;

  AckSourceReader(
      Supplier<SplitReader<AckRecord<OUT>, AckSplit>> splitReaderSupplier,
      CamelRuntime runtime,
      PendingAcks pendingAcks,
      Configuration config,
      SourceReaderContext context) {
    super(splitReaderSupplier, new AckRecordEmitter<>(pendingAcks), config, context);
    this.runtime = runtime;
    this.pendingAcks = pendingAcks;
  }

  @Override
  public List<AckSplit> snapshotState(long checkpointId) {
    pendingAcks.snapshot(checkpointId);
    runtime.checkpointStarted();
    return super.snapshotState(checkpointId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    List<Exchange> done = pendingAcks.complete(checkpointId);
    if (!done.isEmpty()) {
      runtime.ackAsync(done);
    }
  }

  @Override
  protected void onSplitFinished(Map<String, AckSplit> finishedSplitIds) {}

  @Override
  protected AckSplit initializedState(AckSplit split) {
    return split;
  }

  @Override
  protected AckSplit toSplitType(String splitId, AckSplit splitState) {
    return splitState;
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } finally {
      runtime.close();
    }
  }
}
