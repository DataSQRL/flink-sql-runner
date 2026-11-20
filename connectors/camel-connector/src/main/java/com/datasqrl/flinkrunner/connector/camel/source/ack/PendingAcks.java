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

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.camel.Exchange;

/**
 * Exchanges emitted but not yet safe to acknowledge, grouped by the checkpoint that covers them.
 *
 * <p>Only touched from the task's mailbox thread (emitter, snapshotState,
 * notifyCheckpointComplete), so it needs no synchronization.
 */
final class PendingAcks {

  private List<Exchange> current = new ArrayList<>();
  private final NavigableMap<Long, List<Exchange>> pending = new TreeMap<>();

  /** Records an emitted, dropped or failed exchange. */
  void add(Exchange exchange) {
    current.add(exchange);
  }

  /** Everything emitted so far is covered by this checkpoint once it completes. */
  void snapshot(long checkpointId) {
    if (!current.isEmpty()) {
      pending.put(checkpointId, current);
      current = new ArrayList<>();
    }
  }

  /**
   * Returns the exchanges covered by the completed checkpoint, including those of earlier
   * checkpoints that were aborted or whose notification was lost.
   */
  List<Exchange> complete(long checkpointId) {
    NavigableMap<Long, List<Exchange>> done = pending.headMap(checkpointId, true);
    List<Exchange> exchanges = new ArrayList<>();
    done.values().forEach(exchanges::addAll);
    done.clear();
    return exchanges;
  }
}
