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
package com.datasqrl.flinkrunner.connector.camel.table;

import com.datasqrl.flinkrunner.connector.camel.source.RecordContext;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import java.util.List;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;

/**
 * Turns a source's records into table rows: the record's components with Flink's structured-type
 * converter, followed by the metadata columns in the order the planner asked for them.
 */
final class RowConverter implements RecordConverter<Object, RowData> {

  private final DataStructureConverter recordConverter;
  private final List<ReadableMetadata> metadata;

  RowConverter(DataStructureConverter recordConverter, List<ReadableMetadata> metadata) {
    this.recordConverter = recordConverter;
    this.metadata = List.copyOf(metadata);
  }

  @Override
  public void open(SourceReaderContext context) {
    recordConverter.open(
        RuntimeConverter.Context.create(context.getUserCodeClassLoader().asClassLoader()));
  }

  @Override
  public RowData convert(Object value, RecordContext context) {
    RowData physical = (RowData) recordConverter.toInternal(value);
    if (metadata.isEmpty()) {
      return physical;
    }
    GenericRowData metadataRow = new GenericRowData(metadata.size());
    for (int i = 0; i < metadata.size(); i++) {
      metadataRow.setField(i, metadata.get(i).read(context));
    }
    return new JoinedRowData(physical, metadataRow);
  }
}
