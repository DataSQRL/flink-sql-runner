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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * Fails the job once, at the first record after a checkpoint that covered at least {@code
 * minRecords} records completed, so the restore starts from real source state. Chain it right after
 * the source: its snapshot then sees exactly the records the source's snapshot covers.
 */
public final class FailOnceAfterCheckpoint<T> extends RichMapFunction<T, T>
    implements CheckpointedFunction, CheckpointListener {

  private static final AtomicBoolean FAILED = new AtomicBoolean();

  private final int minRecords;
  private final long delayPerRecordMs;
  private final Map<Long, Integer> countAtSnapshot = new HashMap<>();
  private int count;
  private boolean covered;

  public FailOnceAfterCheckpoint(int minRecords) {
    this(minRecords, 0);
  }

  /** Also slows every record down, so checkpoints land in the middle of a unit. */
  public FailOnceAfterCheckpoint(int minRecords, long delayPerRecordMs) {
    this.minRecords = minRecords;
    this.delayPerRecordMs = delayPerRecordMs;
  }

  public static void reset() {
    FAILED.set(false);
  }

  public static boolean failed() {
    return FAILED.get();
  }

  /** A job configuration that restarts once, so the job survives the one failure. */
  public static Configuration withOneRestart() {
    Configuration config = new Configuration();
    config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
    config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ZERO);
    return config;
  }

  @Override
  public T map(T value) throws Exception {
    count++;
    if (covered && !FAILED.getAndSet(true)) {
      throw new IllegalStateException("failing after a checkpoint");
    }
    if (delayPerRecordMs > 0) {
      Thread.sleep(delayPerRecordMs);
    }
    return value;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    countAtSnapshot.put(context.getCheckpointId(), count);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {}

  @Override
  public void notifyCheckpointComplete(long checkpointId) {
    Integer snapshotCount = countAtSnapshot.get(checkpointId);
    if (snapshotCount != null && snapshotCount >= minRecords) {
      covered = true;
    }
  }
}
