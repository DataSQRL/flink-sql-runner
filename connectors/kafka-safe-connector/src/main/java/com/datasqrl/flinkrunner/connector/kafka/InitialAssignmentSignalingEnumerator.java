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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
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
public final class InitialAssignmentSignalingEnumerator<EnumChkT>
    extends ForwardingEnumerator<KafkaPartitionSplit, EnumChkT> {

  private final SplitEnumeratorContext<KafkaPartitionSplit> context;
  private final SplitEnumerator<KafkaPartitionSplit, EnumChkT> delegate;

  /** The async calls of the first assignment whose handler has not returned yet. */
  private int pendingInitialCalls;

  /** Whether the coordinator thread is in {@code start()} or in a handler of such a call. */
  private boolean inInitialAssignment;

  private boolean initialAssignmentDone;

  /**
   * @param enumeratorFactory creates or restores the wrapped enumerator with the context it is
   *     given, through which this enumerator sees its async calls
   */
  public InitialAssignmentSignalingEnumerator(
      SplitEnumeratorContext<KafkaPartitionSplit> context,
      FunctionWithException<
              SplitEnumeratorContext<KafkaPartitionSplit>,
              SplitEnumerator<KafkaPartitionSplit, EnumChkT>,
              Exception>
          enumeratorFactory)
      throws Exception {
    this.context = checkNotNull(context, "Enumerator context must not be null.");
    this.delegate =
        checkNotNull(
            enumeratorFactory.apply(new CallTrackingContext()),
            "Delegate enumerator must not be null.");
  }

  @Override
  protected SplitEnumerator<KafkaPartitionSplit, EnumChkT> delegate() {
    return delegate;
  }

  @Override
  public void start() {
    inInitialAssignment = true;
    try {
      delegate.start();
    } finally {
      inInitialAssignment = false;
    }

    // No handler has run yet: the coordinator runs them on this thread, after start() returns.
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
    if (initialAssignmentDone || pendingInitialCalls > 0) {
      return;
    }

    initialAssignmentDone = true;
    for (int subtaskId : context.registeredReaders().keySet()) {
      context.sendEventToSourceReader(subtaskId, new InitialAssignmentDoneEvent());
    }
  }

  /**
   * The context the wrapped enumerator works with: the coordinator's, except that it counts the
   * async calls of the first assignment.
   */
  private final class CallTrackingContext extends ForwardingEnumeratorContext<KafkaPartitionSplit> {

    private CallTrackingContext() {
      super(context);
    }

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
      var firstRunStarted = new AtomicBoolean();
      return (result, error) -> {
        if (firstRunStarted.getAndSet(true)) {
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
  }
}
