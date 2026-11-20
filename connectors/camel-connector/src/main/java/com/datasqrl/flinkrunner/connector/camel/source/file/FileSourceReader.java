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

import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

/**
 * File-mode source reader. The split state is the seen-set of file versions read so far, so a
 * restore skips them and re-reads a half-emitted unit from the start with the same timestamp.
 */
final class FileSourceReader<OUT>
    extends SingleThreadMultiplexSourceReaderBase<FileRecord<OUT>, OUT, FileSplit, FileSplitState> {

  private final CamelRuntime runtime;

  FileSourceReader(
      Supplier<SplitReader<FileRecord<OUT>, FileSplit>> splitReaderSupplier,
      FileRecordEmitter<OUT> emitter,
      CamelRuntime runtime,
      SourceReaderContext context) {
    super(splitReaderSupplier, emitter, new Configuration(), context);
    this.runtime = runtime;
  }

  @Override
  protected void onSplitFinished(Map<String, FileSplitState> finishedSplitIds) {}

  @Override
  protected FileSplitState initializedState(FileSplit split) {
    return new FileSplitState(split);
  }

  @Override
  protected FileSplit toSplitType(String splitId, FileSplitState splitState) {
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
