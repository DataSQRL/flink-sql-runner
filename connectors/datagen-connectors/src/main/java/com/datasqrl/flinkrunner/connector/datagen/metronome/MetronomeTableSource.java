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
package com.datasqrl.flinkrunner.connector.datagen.metronome;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.types.DataType;

/** Table source adapter that exposes the metronome source to Flink SQL. */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MetronomeTableSource implements ScanTableSource, SupportsLimitPushDown {

  private static final int SOURCE_PARALLELISM = 1;

  private final DataType rowDataType;

  @Nullable private Long numberOfRows;

  public MetronomeTableSource(DataType rowDataType) {
    this(rowDataType, null);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
    return SourceProvider.of(new MetronomeSource(numberOfRows), SOURCE_PARALLELISM);
  }

  @Override
  public DynamicTableSource copy() {
    return new MetronomeTableSource(rowDataType, numberOfRows);
  }

  @Override
  public String asSummaryString() {
    return "MetronomeSource";
  }

  @Override
  public void applyLimit(long limit) {
    this.numberOfRows = limit;
  }
}
