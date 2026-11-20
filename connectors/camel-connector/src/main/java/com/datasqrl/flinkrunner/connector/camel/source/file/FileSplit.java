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

import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollClock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.connector.source.SourceSplit;

/**
 * The single split of a file-mode source: the seen-set of file versions already read, plus what a
 * restore needs to re-read a half-emitted unit with the same timestamp.
 *
 * <p>The seen-set maps a file version ({@code path@mtime@size}, or {@code url@sha256:...}) to the
 * last time a listing contained it. With snapshot unit {@code listing} it holds the versions of the
 * last completed listing.
 */
public final class FileSplit implements SourceSplit {

  private final String splitId;
  private final Map<String, Long> seen;
  private final long lastT;
  private final long unitT;
  private final String unitKey;
  private final long lastUnitRows;

  /**
   * @param lastT the timestamp of the last completed unit, or {@link PollClock#NONE}
   * @param unitT the timestamp of the unit in progress, or {@link PollClock#NONE}
   * @param unitKey the version being read, for snapshot unit {@code file}; otherwise {@code null}
   * @param lastUnitRows the rows of the last completed unit, or -1
   */
  FileSplit(
      String splitId,
      Map<String, Long> seen,
      long lastT,
      long unitT,
      String unitKey,
      long lastUnitRows) {
    this.splitId = splitId;
    this.seen = Collections.unmodifiableMap(new HashMap<>(seen));
    this.lastT = lastT;
    this.unitT = unitT;
    this.unitKey = unitKey;
    this.lastUnitRows = lastUnitRows;
  }

  static FileSplit initial() {
    return new FileSplit(
        CamelSplitEnumerator.SPLIT_ID, Map.of(), PollClock.NONE, PollClock.NONE, null, -1);
  }

  @Override
  public String splitId() {
    return splitId;
  }

  Map<String, Long> seen() {
    return seen;
  }

  long lastT() {
    return lastT;
  }

  long unitT() {
    return unitT;
  }

  String unitKey() {
    return unitKey;
  }

  long lastUnitRows() {
    return lastUnitRows;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FileSplit other
        && splitId.equals(other.splitId)
        && seen.equals(other.seen)
        && lastT == other.lastT
        && unitT == other.unitT
        && Objects.equals(unitKey, other.unitKey)
        && lastUnitRows == other.lastUnitRows;
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitId, seen, lastT, unitT, unitKey, lastUnitRows);
  }

  @Override
  public String toString() {
    return "FileSplit{"
        + splitId
        + ", seen="
        + seen.size()
        + " versions, lastT="
        + lastT
        + ", unitT="
        + unitT
        + ", unitKey="
        + unitKey
        + "}";
  }
}
