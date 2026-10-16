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
package com.datasqrl.flinkrunner.format.csv;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.DeserializationSchemaAdapter;
import org.apache.flink.table.data.RowData;

/**
 * Reads CSV lines the same way the filesystem connector reads any {@link DeserializationSchema}
 * format, through a {@link DeserializationSchemaAdapter}, but skips the header only in the split
 * that starts at the beginning of a file. A file that Flink reads in several splits therefore keeps
 * the first line of every later split.
 */
@RequiredArgsConstructor
public class FlexibleCsvBulkFormat implements BulkFormat<RowData, FileSourceSplit> {

  private final DeserializationSchema<RowData> deserializer;
  private final boolean skipHeader;

  @Override
  public Reader<RowData> createReader(Configuration config, FileSourceSplit split)
      throws IOException {
    return lineFormat(split).createReader(config, split);
  }

  @Override
  public Reader<RowData> restoreReader(Configuration config, FileSourceSplit split)
      throws IOException {
    // The restored reader skips the header again before it skips the checkpointed records, which
    // never counted the header
    return lineFormat(split).restoreReader(config, split);
  }

  @Override
  public boolean isSplittable() {
    return true;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  private DeserializationSchemaAdapter lineFormat(FileSourceSplit split) {
    var startsAtHeader = skipHeader && split.offset() == 0;
    return new DeserializationSchemaAdapter(
        startsAtHeader ? new FlexibleCsv.RuntimeDecoderDelegate(deserializer, true) : deserializer);
  }
}
