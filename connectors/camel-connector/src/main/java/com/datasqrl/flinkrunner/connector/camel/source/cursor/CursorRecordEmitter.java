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

import com.datasqrl.flinkrunner.connector.camel.source.polling.UnitWatermarks;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * Emits cursor records and moves the split's position to the one each record carries. Runs on the
 * mailbox thread, so the next snapshot holds exactly the position after the last emitted record.
 */
final class CursorRecordEmitter<OUT, POS>
    implements RecordEmitter<CursorRecord<OUT, POS>, OUT, CursorSplitState<POS>> {

  private final UnitWatermarks watermarks;
  private boolean rowsInUnit;

  CursorRecordEmitter(boolean pollWatermarks) {
    this.watermarks = new UnitWatermarks(pollWatermarks);
  }

  @Override
  public void emitRecord(
      CursorRecord<OUT, POS> record, SourceOutput<OUT> output, CursorSplitState<POS> state) {
    switch (record.kind()) {
      case UNIT_START -> {
        // A poll resumed after a restore already emitted rows at this timestamp.
        rowsInUnit = state.unitT() == record.timestamp();
        state.unitStarted(record.timestamp());
      }
      case RECORD -> {
        // A record the converter dropped or failed on still moves the position past it.
        if (record.value() != null) {
          watermarks.beforeRecord(output);
          output.collect(record.value(), record.timestamp());
          rowsInUnit = true;
        }
        state.emitted(record.position());
      }
      case UNIT_END -> {
        state.unitCompleted(record.timestamp());
        watermarks.unitComplete(output, record.timestamp(), rowsInUnit);
        rowsInUnit = false;
      }
    }
  }
}
