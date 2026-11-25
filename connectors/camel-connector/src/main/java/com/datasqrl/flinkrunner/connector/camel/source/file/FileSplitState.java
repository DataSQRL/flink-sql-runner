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
package com.datasqrl.flinkrunner.connector.camel.source.file;

import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The mutable state of a file split. Only the emitter changes it, on the mailbox thread, so a
 * version enters the seen-set exactly when its last record has been emitted.
 */
final class FileSplitState {

  private final String splitId;
  private final Map<String, Long> seen;
  private long lastT;
  private long unitT;
  private String unitKey;
  private long lastUnitRows;

  FileSplitState(FileSplit split) {
    this.splitId = split.splitId();
    this.seen = new HashMap<>(split.seen());
    this.lastT = split.lastT();
    this.unitT = split.unitT();
    this.unitKey = split.unitKey();
    this.lastUnitRows = split.lastUnitRows();
  }

  long unitT() {
    return unitT;
  }

  void unitStarted(long t, String key) {
    unitT = t;
    unitKey = key;
  }

  /** A file unit is fully emitted: its version is seen from now on. */
  void fileCompleted(long t, String key, long rows, long at) {
    seen.put(key, at);
    unitCompleted(t, rows);
  }

  /** A listing unit is fully emitted: the seen-set becomes that listing. */
  void listingCompleted(long t, Set<String> keys, long rows, long at) {
    seen.clear();
    keys.forEach(key -> seen.put(key, at));
    unitCompleted(t, rows);
  }

  private void unitCompleted(long t, long rows) {
    lastT = Math.max(lastT, t);
    unitT = PollClock.NONE;
    unitKey = null;
    lastUnitRows = rows;
  }

  /** A unit that can't be completed, because its file is gone. */
  void unitAbandoned(long t) {
    lastT = Math.max(lastT, t);
    unitT = PollClock.NONE;
    unitKey = null;
  }

  /**
   * A listing contained these versions at {@code at}. Versions no listing has contained for {@code
   * retentionMs} are evicted.
   */
  void listed(Set<String> keys, long at, long retentionMs) {
    for (String key : keys) {
      seen.computeIfPresent(key, (k, lastListed) -> at);
    }
    seen.values().removeIf(lastListed -> lastListed < at - retentionMs);
  }

  FileSplit toSplit() {
    return new FileSplit(splitId, seen, lastT, unitT, unitKey, lastUnitRows);
  }
}
