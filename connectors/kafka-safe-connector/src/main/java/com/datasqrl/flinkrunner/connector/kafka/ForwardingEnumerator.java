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

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;

/**
 * An enumerator that forwards every call to the enumerator it wraps, so that a wrapper overrides
 * only what it changes.
 *
 * <p>The wrapped enumerator comes from {@link #delegate()}, not from the constructor as in {@link
 * ForwardingSource}: a wrapper may have to create it with a context of its own, which it can only
 * build once it is constructed itself.
 */
@Internal
public abstract class ForwardingEnumerator<SplitT extends SourceSplit, EnumChkT>
    implements SplitEnumerator<SplitT, EnumChkT> {

  protected abstract SplitEnumerator<SplitT, EnumChkT> delegate();

  @Override
  public void start() {
    delegate().start();
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    delegate().handleSplitRequest(subtaskId, requesterHostname);
  }

  @Override
  public void addSplitsBack(List<SplitT> splits, int subtaskId) {
    delegate().addSplitsBack(splits, subtaskId);
  }

  @Override
  public void addReader(int subtaskId) {
    delegate().addReader(subtaskId);
  }

  @Override
  public EnumChkT snapshotState(long checkpointId) throws Exception {
    return delegate().snapshotState(checkpointId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    delegate().notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    delegate().notifyCheckpointAborted(checkpointId);
  }

  @Override
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    delegate().handleSourceEvent(subtaskId, sourceEvent);
  }

  @Override
  public void close() throws IOException {
    delegate().close();
  }
}
