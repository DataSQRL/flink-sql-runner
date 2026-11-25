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
package com.datasqrl.flinkrunner.connector.camel.source.cursor;

import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;

/**
 * The mutable state of a cursor split. Only the emitter changes it, on the mailbox thread, so a
 * snapshot always matches what was emitted.
 */
final class CursorSplitState<POS> {

  private final String splitId;
  private POS position;
  private long lastT;
  private long unitT;

  CursorSplitState(CursorSplit<POS> split) {
    this.splitId = split.splitId();
    this.position = split.position();
    this.lastT = split.lastT();
    this.unitT = split.unitT();
  }

  long unitT() {
    return unitT;
  }

  void unitStarted(long t) {
    unitT = t;
  }

  void emitted(POS position) {
    this.position = position;
  }

  void unitCompleted(long t) {
    lastT = Math.max(lastT, t);
    unitT = PollClock.NONE;
  }

  CursorSplit<POS> toSplit() {
    return new CursorSplit<>(splitId, position, lastT, unitT);
  }
}
