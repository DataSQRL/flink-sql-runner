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

import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

/**
 * Cursor-mode source reader. The split state is the highest position emitted, so a restore
 * re-requests from there. Boundary records are replayed and deduplicated downstream.
 */
final class CursorSourceReader<OUT, POS>
    extends SingleThreadMultiplexSourceReaderBase<
        CursorRecord<OUT, POS>, OUT, CursorSplit<POS>, CursorSplitState<POS>> {

  private final CamelRuntime runtime;

  CursorSourceReader(
      Supplier<SplitReader<CursorRecord<OUT, POS>, CursorSplit<POS>>> splitReaderSupplier,
      boolean pollWatermarks,
      CamelRuntime runtime,
      SourceReaderContext context) {
    super(
        splitReaderSupplier,
        new CursorRecordEmitter<>(pollWatermarks),
        new Configuration(),
        context);
    this.runtime = runtime;
  }

  @Override
  protected void onSplitFinished(Map<String, CursorSplitState<POS>> finishedSplitIds) {}

  @Override
  protected CursorSplitState<POS> initializedState(CursorSplit<POS> split) {
    return new CursorSplitState<>(split);
  }

  @Override
  protected CursorSplit<POS> toSplitType(String splitId, CursorSplitState<POS> splitState) {
    return splitState.toSplit();
  }

  @Override
  public void close() throws Exception {
    try {
      super.close();
    } finally {
      runtime.close();
    }
  }
}
