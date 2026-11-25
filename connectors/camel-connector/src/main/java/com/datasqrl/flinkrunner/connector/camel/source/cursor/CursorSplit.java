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

import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

/**
 * The single split of a cursor-mode source: the position of the last emitted record, plus the
 * timestamps that keep T monotonic and let a restore re-run a half-emitted poll with the same T.
 */
public final class CursorSplit<POS> implements SourceSplit {

  private final String splitId;
  private final POS position;
  private final long lastT;
  private final long unitT;

  /**
   * @param position the position of the last emitted record, or {@code null} before the first
   * @param lastT the timestamp of the last completed poll, or {@link PollClock#NONE}
   * @param unitT the timestamp of the poll in progress, or {@link PollClock#NONE}
   */
  CursorSplit(String splitId, POS position, long lastT, long unitT) {
    this.splitId = splitId;
    this.position = position;
    this.lastT = lastT;
    this.unitT = unitT;
  }

  static <POS> CursorSplit<POS> initial() {
    return new CursorSplit<>(CamelSplitEnumerator.SPLIT_ID, null, PollClock.NONE, PollClock.NONE);
  }

  @Override
  public String splitId() {
    return splitId;
  }

  POS position() {
    return position;
  }

  long lastT() {
    return lastT;
  }

  long unitT() {
    return unitT;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof CursorSplit<?> other
        && splitId.equals(other.splitId)
        && Objects.equals(position, other.position)
        && lastT == other.lastT
        && unitT == other.unitT;
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitId, position, lastT, unitT);
  }

  @Override
  public String toString() {
    return "CursorSplit{"
        + splitId
        + ", position="
        + position
        + ", lastT="
        + lastT
        + ", unitT="
        + unitT
        + "}";
  }
}
