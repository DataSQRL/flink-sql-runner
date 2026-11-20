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

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/**
 * Emits mapped values and hands every exchange, emitted or dropped, to {@link PendingAcks}. Runs on
 * the mailbox thread, so the next snapshot covers exactly what was emitted before it.
 *
 * <p>Records carry their poll timestamp. When the fetcher marks the end of a poll, the emitter
 * emits a watermark at that timestamp and marks the output idle until the next poll's first record,
 * so a quiet endpoint never holds back downstream event time.
 */
final class AckRecordEmitter<OUT> implements RecordEmitter<AckRecord<OUT>, OUT, AckSplit> {

  private final PendingAcks pendingAcks;
  private boolean idle;

  AckRecordEmitter(PendingAcks pendingAcks) {
    this.pendingAcks = pendingAcks;
  }

  @Override
  public void emitRecord(AckRecord<OUT> record, SourceOutput<OUT> output, AckSplit split) {
    if (record.isEndOfPoll()) {
      // Every record of the poll was stamped with this timestamp and emitted before it.
      output.emitWatermark(new Watermark(record.timestamp()));
      output.markIdle();
      idle = true;
      return;
    }
    if (idle) {
      output.markActive();
      idle = false;
    }
    if (record.value() != null) {
      output.collect(record.value(), record.timestamp());
    }
    pendingAcks.add(record.exchange());
  }
}
