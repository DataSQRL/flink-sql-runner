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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

/**
 * An enumerator context that forwards every call to the context it wraps, so that a wrapper
 * overrides only what it changes.
 */
@Internal
public abstract class ForwardingEnumeratorContext<SplitT extends SourceSplit>
    implements SplitEnumeratorContext<SplitT> {

  protected final SplitEnumeratorContext<SplitT> delegate;

  protected ForwardingEnumeratorContext(SplitEnumeratorContext<SplitT> delegate) {
    this.delegate = checkNotNull(delegate, "Delegate enumerator context must not be null.");
  }

  @Override
  public SplitEnumeratorMetricGroup metricGroup() {
    return delegate.metricGroup();
  }

  @Override
  public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
    delegate.sendEventToSourceReader(subtaskId, event);
  }

  @Override
  public void sendEventToSourceReader(int subtaskId, int attemptNumber, SourceEvent event) {
    delegate.sendEventToSourceReader(subtaskId, attemptNumber, event);
  }

  @Override
  public int currentParallelism() {
    return delegate.currentParallelism();
  }

  @Override
  public Map<Integer, ReaderInfo> registeredReaders() {
    return delegate.registeredReaders();
  }

  @Override
  public Map<Integer, Map<Integer, ReaderInfo>> registeredReadersOfAttempts() {
    return delegate.registeredReadersOfAttempts();
  }

  @Override
  public void assignSplits(SplitsAssignment<SplitT> newSplitAssignments) {
    delegate.assignSplits(newSplitAssignments);
  }

  @Override
  public void assignSplit(SplitT split, int subtask) {
    delegate.assignSplit(split, subtask);
  }

  @Override
  public void signalNoMoreSplits(int subtask) {
    delegate.signalNoMoreSplits(subtask);
  }

  @Override
  public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
    delegate.callAsync(callable, handler);
  }

  @Override
  public <T> void callAsync(
      Callable<T> callable,
      BiConsumer<T, Throwable> handler,
      long initialDelayMillis,
      long periodMillis) {
    delegate.callAsync(callable, handler, initialDelayMillis, periodMillis);
  }

  @Override
  public void runInCoordinatorThread(Runnable runnable) {
    delegate.runInCoordinatorThread(runnable);
  }

  @Override
  public void setIsProcessingBacklog(boolean isProcessingBacklog) {
    delegate.setIsProcessingBacklog(isProcessingBacklog);
  }
}
