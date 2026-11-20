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

import com.datasqrl.flinkrunner.connector.camel.source.polling.UnitWatermarks;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * Emits file records and applies unit boundaries to the split state. Runs on the mailbox thread, so
 * a snapshot never holds a version in the seen-set whose records weren't all emitted.
 */
final class FileRecordEmitter<OUT> implements RecordEmitter<FileRecord<OUT>, OUT, FileSplitState> {

  private final CamelFileSource.SnapshotUnit snapshotUnit;
  private final long seenRetentionMs;
  private final UnitWatermarks watermarks;
  private boolean rowsInUnit;

  FileRecordEmitter(
      CamelFileSource.SnapshotUnit snapshotUnit, long seenRetentionMs, boolean pollWatermarks) {
    this.snapshotUnit = snapshotUnit;
    this.seenRetentionMs = seenRetentionMs;
    this.watermarks = new UnitWatermarks(pollWatermarks);
  }

  @Override
  public void emitRecord(FileRecord<OUT> record, SourceOutput<OUT> output, FileSplitState state) {
    switch (record.kind()) {
      case UNIT_START -> {
        // A unit resumed after a restore already emitted rows at this timestamp.
        rowsInUnit = state.unitT() == record.timestamp();
        state.unitStarted(record.timestamp(), record.key());
      }
      case RECORD -> {
        watermarks.beforeRecord(output);
        output.collect(record.value(), record.timestamp());
        rowsInUnit = true;
      }
      case UNIT_END -> {
        if (snapshotUnit == CamelFileSource.SnapshotUnit.LISTING) {
          state.listingCompleted(record.timestamp(), record.keys(), record.rows(), record.at());
        } else {
          state.fileCompleted(
              record.timestamp(), record.keys().iterator().next(), record.rows(), record.at());
        }
        watermarks.unitComplete(output, record.timestamp(), rowsInUnit);
        rowsInUnit = false;
      }
      case UNIT_ABANDONED -> {
        state.unitAbandoned(record.timestamp());
        rowsInUnit = false;
      }
      case LISTED -> state.listed(record.keys(), record.at(), seenRetentionMs);
    }
  }
}
