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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.util.function.FunctionWithException;

/**
 * An enumerator that sends each reader an {@link InitialAssignmentDoneEvent} once the enumerator it
 * wraps has assigned the splits of its first discovery.
 *
 * <p>Flink tells a reader which splits it gets, but not that it gets none, so a reader without
 * splits cannot tell whether the enumerator is still about to assign it one. The enumerator has
 * finished its first assignment once every async call it made from {@code start()}, and every one
 * the handlers of those calls made, has been handled: a Kafka enumerator lists the partitions in
 * one call, and looks up where their splits start and assigns them in the handler of a second.
 * Readers registered by then get the event at that point, and readers that register later get it
 * after the splits that were pending for them.
 *
 * <p>If a handler of those calls fails, no reader gets the event; such a failure fails the job.
 */
@Internal
public final class InitialAssignmentSignalingEnumerator<SplitT extends SourceSplit, EnumChkT>
    implements SplitEnumerator<SplitT, EnumChkT> {

  private final SplitEnumeratorContext<SplitT> context;
  private final SplitEnumerator<SplitT, EnumChkT> delegate;

  /** The async calls of the first assignment whose handler has not returned yet. */
  private int pendingInitialCalls;

  /** Whether the coordinator thread is in {@code start()} or in a handler of such a call. */
  private boolean inInitialAssignment;

  private boolean started;
  private boolean initialAssignmentDone;

  /**
   * @param enumeratorFactory creates or restores the wrapped enumerator with the context it is
   *     given, through which this enumerator sees its async calls
   */
  public InitialAssignmentSignalingEnumerator(
      SplitEnumeratorContext<SplitT> context,
      FunctionWithException<
              SplitEnumeratorContext<SplitT>, SplitEnumerator<SplitT, EnumChkT>, Exception>
          enumeratorFactory)
      throws Exception {
    this.context = checkNotNull(context, "Enumerator context must not be null.");
    this.delegate =
        checkNotNull(
            enumeratorFactory.apply(new CallTrackingContext()),
            "Delegate enumerator must not be null.");
  }

  @Override
  public void start() {
    inInitialAssignment = true;
    try {
      delegate.start();
    } finally {
      inInitialAssignment = false;
    }

    started = true;
    signalIfInitialAssignmentDone();
  }

  @Override
  public void addReader(int subtaskId) {
    // Assigns the reader the splits that were pending for it, ahead of the event.
    delegate.addReader(subtaskId);
    if (initialAssignmentDone) {
      context.sendEventToSourceReader(subtaskId, new InitialAssignmentDoneEvent());
    }
  }

  private void signalIfInitialAssignmentDone() {
    if (initialAssignmentDone || !started || pendingInitialCalls > 0) {
      return;
    }

    initialAssignmentDone = true;
    for (int subtaskId : context.registeredReaders().keySet()) {
      context.sendEventToSourceReader(subtaskId, new InitialAssignmentDoneEvent());
    }
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    delegate.handleSplitRequest(subtaskId, requesterHostname);
  }

  @Override
  public void addSplitsBack(List<SplitT> splits, int subtaskId) {
    delegate.addSplitsBack(splits, subtaskId);
  }

  @Override
  public EnumChkT snapshotState(long checkpointId) throws Exception {
    return delegate.snapshotState(checkpointId);
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
  public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
    delegate.handleSourceEvent(subtaskId, sourceEvent);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * The context the wrapped enumerator works with: the coordinator's, except that it counts the
   * async calls of the first assignment.
   */
  private final class CallTrackingContext implements SplitEnumeratorContext<SplitT> {

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
      context.callAsync(callable, tracked(handler));
    }

    @Override
    public <T> void callAsync(
        Callable<T> callable,
        BiConsumer<T, Throwable> handler,
        long initialDelayMillis,
        long periodMillis) {
      // Only the first run of a periodic call belongs to the first assignment.
      context.callAsync(callable, tracked(handler), initialDelayMillis, periodMillis);
    }

    /** Counts {@code handler}'s first run, if the call is made during the first assignment. */
    private <T> BiConsumer<T, Throwable> tracked(BiConsumer<T, Throwable> handler) {
      if (!inInitialAssignment) {
        return handler;
      }

      pendingInitialCalls++;
      var ran = new AtomicBoolean();
      return (result, error) -> {
        if (ran.getAndSet(true)) {
          handler.accept(result, error);
          return;
        }

        inInitialAssignment = true;
        try {
          handler.accept(result, error);
        } finally {
          inInitialAssignment = false;
        }

        // Not reached if the handler throws: the call then stays pending.
        pendingInitialCalls--;
        signalIfInitialAssignmentDone();
      };
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
      return context.metricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
      context.sendEventToSourceReader(subtaskId, event);
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
      context.sendEventToSourceReader(subtaskId, attemptNumber, event);
    }

    @Override
    public int currentParallelism() {
      return context.currentParallelism();
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
      return context.registeredReaders();
    }

    @Override
    public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
      return context.registeredReadersOfAttempts();
    }

    @Override
    public void assignSplits(SplitsAssignment<SplitT> newSplitAssignments) {
      context.assignSplits(newSplitAssignments);
    }

    @Override
    public void assignSplit(SplitT split, int subtask) {
      context.assignSplit(split, subtask);
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
      context.signalNoMoreSplits(subtask);
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
      context.runInCoordinatorThread(runnable);
    }

    @Override
    public void setIsProcessingBacklog(boolean isProcessingBacklog) {
      context.setIsProcessingBacklog(isProcessingBacklog);
    }
  }
}
