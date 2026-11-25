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
package com.datasqrl.flinkrunner.connector.camel.source;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

/**
 * Assigns the single split of a Camel source to subtask 0. Every other subtask gets no split and
 * finishes straight away, so the effective source parallelism is always 1. Used by every mode; only
 * the split type differs.
 */
@Internal
public final class CamelSplitEnumerator<SplitT extends SourceSplit>
    implements SplitEnumerator<SplitT, List<SplitT>> {

  /** The ID of the single split, in every mode. */
  public static final String SPLIT_ID = "camel-0";

  private static final int OWNER_SUBTASK = 0;

  private final SplitEnumeratorContext<SplitT> context;
  private final List<SplitT> unassigned;

  public CamelSplitEnumerator(SplitEnumeratorContext<SplitT> context, List<SplitT> unassigned) {
    this.context = context;
    this.unassigned = new ArrayList<>(unassigned);
  }

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, String requesterHostname) {}

  @Override
  public void addSplitsBack(List<SplitT> splits, int subtaskId) {
    // The owner failed before a checkpoint recorded the split. It gets the split again when
    // it re-registers.
    unassigned.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    if (subtaskId == OWNER_SUBTASK) {
      for (SplitT split : unassigned) {
        context.assignSplit(split, subtaskId);
      }
      unassigned.clear();
    }
    context.signalNoMoreSplits(subtaskId);
  }

  @Override
  public List<SplitT> snapshotState(long checkpointId) {
    return new ArrayList<>(unassigned);
  }

  @Override
  public void close() {}
}
