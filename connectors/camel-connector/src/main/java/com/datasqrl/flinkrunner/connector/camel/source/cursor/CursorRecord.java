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

/**
 * What the cursor fetcher hands to the emitter: a mapped record with the split's position after it,
 * or the start or end of a poll. Every record of one poll carries the poll's timestamp. A record's
 * value is {@code null} when the converter failed on it; its position still counts.
 */
record CursorRecord<OUT, POS>(Kind kind, OUT value, POS position, long timestamp) {

  enum Kind {
    UNIT_START,
    RECORD,
    UNIT_END
  }

  static <OUT, POS> CursorRecord<OUT, POS> unitStart(long timestamp) {
    return new CursorRecord<>(Kind.UNIT_START, null, null, timestamp);
  }

  static <OUT, POS> CursorRecord<OUT, POS> record(OUT value, POS position, long timestamp) {
    return new CursorRecord<>(Kind.RECORD, value, position, timestamp);
  }

  static <OUT, POS> CursorRecord<OUT, POS> unitEnd(long timestamp) {
    return new CursorRecord<>(Kind.UNIT_END, null, null, timestamp);
  }
}
