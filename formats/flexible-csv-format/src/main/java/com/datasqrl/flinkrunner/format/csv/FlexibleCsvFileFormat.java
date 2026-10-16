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

import com.google.auto.service.AutoService;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.formats.csv.CsvFormatFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

/**
 * Reads {@code flexible-csv} files for the filesystem connector, which prefers this factory over
 * {@link FlexibleCsv}. Lines are decoded exactly like {@link FlexibleCsv} does, but the header is
 * skipped once per file instead of once per reader.
 */
@AutoService(Factory.class)
public class FlexibleCsvFileFormat implements BulkReaderFormatFactory {

  private final CsvFormatFactory csvFormat = new CsvFormatFactory();

  @Override
  @SuppressWarnings("unchecked")
  public BulkDecodingFormat<RowData> createDecodingFormat(
      DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    var csvDecodingFormat =
        (ProjectableDecodingFormat<DeserializationSchema<RowData>>)
            csvFormat.createDecodingFormat(context, formatOptions);

    return new FileDecodingFormat(csvDecodingFormat, formatOptions.get(FlexibleCsv.SKIP_HEADER));
  }

  @Override
  public String factoryIdentifier() {
    return FlexibleCsv.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(FlexibleCsv.SKIP_HEADER);
  }

  private record FileDecodingFormat(
      ProjectableDecodingFormat<DeserializationSchema<RowData>> csvDecodingFormat,
      boolean skipHeader)
      implements BulkDecodingFormat<RowData>,
          ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>> {

    @Override
    public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
        DynamicTableSource.Context context, DataType physicalDataType, int[][] projections) {
      return new FlexibleCsvBulkFormat(
          csvDecodingFormat.createRuntimeDecoder(context, physicalDataType, projections),
          skipHeader);
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return ChangelogMode.insertOnly();
    }
  }
}
