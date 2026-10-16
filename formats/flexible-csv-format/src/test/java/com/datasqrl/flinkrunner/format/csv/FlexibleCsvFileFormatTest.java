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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class FlexibleCsvFileFormatTest {

  private static final DataType ROW_TYPE =
      DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("name", DataTypes.STRING()));

  private static final String DATA = "1,a\n2,b\n3,c\n4,d\n";
  private static final String CSV_WITH_HEADER = "id,name\n" + DATA;
  private static final List<String> DATA_ROWS = List.of("1:a", "2:b", "3:c", "4:d");

  @TempDir Path tempDir;

  @Test
  void
      givenFlexibleCsvIdentifier_whenDiscoverFactories_thenFilesUseBulkFormatAndMessagesUseDeserializer() {
    var classLoader = getClass().getClassLoader();

    assertThat(
            FactoryUtil.discoverFactory(
                classLoader, BulkReaderFormatFactory.class, FlexibleCsv.IDENTIFIER))
        .isInstanceOf(FlexibleCsvFileFormat.class);
    assertThat(
            FactoryUtil.discoverFactory(
                classLoader, DeserializationFormatFactory.class, FlexibleCsv.IDENTIFIER))
        .isInstanceOf(FlexibleCsv.class);
  }

  @Test
  void givenFileWithHeader_whenReadInOneSplit_thenSkipsHeader() throws IOException {
    var file = write(CSV_WITH_HEADER);

    assertThat(read(format(true), wholeFile(file))).containsExactlyElementsOf(DATA_ROWS);
  }

  @ParameterizedTest
  @MethodSource("splitPoints")
  void givenFileWithHeader_whenReadInTwoSplits_thenReadsEveryDataRowOnce(int splitPoint)
      throws IOException {
    var file = write(CSV_WITH_HEADER);
    var format = format(true);

    var rows = new ArrayList<String>();
    rows.addAll(read(format, split(file, 0, splitPoint)));
    rows.addAll(read(format, split(file, splitPoint, CSV_WITH_HEADER.length() - splitPoint)));

    assertThat(rows).containsExactlyElementsOf(DATA_ROWS);
  }

  @Test
  void givenSkipHeaderDisabled_whenRead_thenKeepsFirstLine() throws IOException {
    var file = write(DATA);

    assertThat(read(format(false), wholeFile(file))).containsExactlyElementsOf(DATA_ROWS);
  }

  @Test
  void givenRestoredFirstSplit_whenRead_thenSkipsHeaderAndCheckpointedRecords() throws IOException {
    var file = write(CSV_WITH_HEADER);
    var restored =
        wholeFile(file)
            .updateWithCheckpointedPosition(
                new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 2));

    assertThat(restore(format(true), restored)).containsExactly("3:c", "4:d");
  }

  @Test
  void givenRestoredLaterSplit_whenRead_thenKeepsItsFirstLine() throws IOException {
    var file = write(CSV_WITH_HEADER);
    var splitPoint = CSV_WITH_HEADER.indexOf("2,b") + 1;
    var restored =
        split(file, splitPoint, CSV_WITH_HEADER.length() - splitPoint)
            .updateWithCheckpointedPosition(
                new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, 1));

    assertThat(restore(format(true), restored)).containsExactly("4:d");
  }

  static IntStream splitPoints() {
    return IntStream.range(1, CSV_WITH_HEADER.length());
  }

  @SuppressWarnings("unchecked")
  private static BulkFormat<RowData, FileSourceSplit> format(boolean skipHeader) {
    var options = new Configuration();
    options.set(FlexibleCsv.SKIP_HEADER, skipHeader);
    var decodingFormat =
        (ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>>)
            new FlexibleCsvFileFormat().createDecodingFormat(null, options);
    var context = mock(DynamicTableSource.Context.class);
    when(context.<RowData>createTypeInformation(any(LogicalType.class)))
        .thenReturn(TypeInformation.of(RowData.class));
    return decodingFormat.createRuntimeDecoder(context, ROW_TYPE, new int[][] {{0}, {1}});
  }

  private Path write(String content) throws IOException {
    return Files.writeString(tempDir.resolve("data.csv"), content);
  }

  private static FileSourceSplit wholeFile(Path file) {
    return split(file, 0, file.toFile().length());
  }

  private static FileSourceSplit split(Path file, long offset, long length) {
    return new FileSourceSplit(
        "split-" + offset,
        org.apache.flink.core.fs.Path.fromLocalFile(file.toFile()),
        offset,
        length,
        file.toFile().lastModified(),
        file.toFile().length());
  }

  private static List<String> read(
      BulkFormat<RowData, FileSourceSplit> format, FileSourceSplit split) throws IOException {
    return collect(format.createReader(new Configuration(), split));
  }

  private static List<String> restore(
      BulkFormat<RowData, FileSourceSplit> format, FileSourceSplit split) throws IOException {
    return collect(format.restoreReader(new Configuration(), split));
  }

  private static List<String> collect(BulkFormat.Reader<RowData> reader) throws IOException {
    var rows = new ArrayList<String>();
    try (reader) {
      BulkFormat.RecordIterator<RowData> batch;
      while ((batch = reader.readBatch()) != null) {
        for (var record = batch.next(); record != null; record = batch.next()) {
          var row = record.getRecord();
          rows.add(row.getInt(0) + ":" + row.getString(1));
        }
        batch.releaseBatch();
      }
    }
    return rows;
  }
}
