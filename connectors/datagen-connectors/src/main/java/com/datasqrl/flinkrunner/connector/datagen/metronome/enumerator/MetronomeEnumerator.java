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
package com.datasqrl.flinkrunner.connector.datagen.metronome.enumerator;

import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplit;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * Enumerator that owns the single metronome split and reassigns pending split state on recovery.
 */
@AllArgsConstructor
public final class MetronomeEnumerator
    implements SplitEnumerator<MetronomeSplit, MetronomeEnumeratorState> {

  private final SplitEnumeratorContext<MetronomeSplit> context;
  private MetronomeEnumeratorState state;

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (state.getPendingSplit() != null) {
      context.assignSplit(state.getPendingSplit(), subtaskId);
      state = MetronomeEnumeratorState.assigned();

    } else if (!state.isAssigned()) {
      context.assignSplit(MetronomeSplit.initial(), subtaskId);
      state = MetronomeEnumeratorState.assigned();
    }

    // Since we only have 1 split every time, there will be no more splits even right after
    // assignment.
    context.signalNoMoreSplits(subtaskId);
  }

  @Override
  public void addSplitsBack(List<MetronomeSplit> splits, int subtaskId) {
    if (!splits.isEmpty()) {
      state = MetronomeEnumeratorState.pending(splits.get(0));
    }
  }

  @Override
  public void addReader(int subtaskId) {}

  @Override
  public MetronomeEnumeratorState snapshotState(long checkpointId) {
    return state;
  }

  @Override
  public void close() {}
}
