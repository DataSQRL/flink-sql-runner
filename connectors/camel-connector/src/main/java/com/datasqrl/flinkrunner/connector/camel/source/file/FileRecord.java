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

import java.util.Set;

/**
 * What the file fetcher hands to the emitter: a mapped record, or a change to the split state at a
 * unit or listing boundary. {@code at} is the fetcher's wall-clock time, so the fetcher's and the
 * emitter's seen-sets age identically.
 */
record FileRecord<OUT>(
    Kind kind, OUT value, long timestamp, String key, Set<String> keys, long rows, long at) {

  enum Kind {
    /** A unit starts at {@code timestamp}; {@code key} is its file for snapshot unit file. */
    UNIT_START,
    RECORD,
    /** The unit at {@code timestamp} is complete: {@code keys} were read, {@code rows} rows. */
    UNIT_END,
    /** The unit at {@code timestamp} can't be completed and is given up. */
    UNIT_ABANDONED,
    /** A listing at {@code at} contained {@code keys}. */
    LISTED
  }

  static <OUT> FileRecord<OUT> unitStart(long timestamp, String key) {
    return new FileRecord<>(Kind.UNIT_START, null, timestamp, key, null, 0, 0);
  }

  static <OUT> FileRecord<OUT> record(OUT value, long timestamp) {
    return new FileRecord<>(Kind.RECORD, value, timestamp, null, null, 0, 0);
  }

  static <OUT> FileRecord<OUT> unitEnd(long timestamp, Set<String> keys, long rows, long at) {
    return new FileRecord<>(Kind.UNIT_END, null, timestamp, null, keys, rows, at);
  }

  static <OUT> FileRecord<OUT> unitAbandoned(long timestamp) {
    return new FileRecord<>(Kind.UNIT_ABANDONED, null, timestamp, null, null, 0, 0);
  }

  static <OUT> FileRecord<OUT> listed(Set<String> keys, long at) {
    return new FileRecord<>(Kind.LISTED, null, 0, null, keys, 0, at);
  }
}
