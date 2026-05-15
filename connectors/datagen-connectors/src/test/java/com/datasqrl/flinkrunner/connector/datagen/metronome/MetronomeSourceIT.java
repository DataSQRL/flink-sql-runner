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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplit;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/** Integration tests for the metronome SQL connector. */
@ExtendWith(MiniClusterExtension.class)
class MetronomeSourceIT { // extends TableITCaseBase {

  private TableEnvironment tEnv;

  @BeforeEach
  void beforeEach() {
    tEnv = TableEnvironmentImpl.create(EnvironmentSettings.newInstance().inStreamingMode().build());
    tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
  }

  @Test
  @Timeout(30)
  void happyPathEmitsMonotonicSequence() throws Exception {
    tEnv.executeSql(
        """
            CREATE TABLE metronome_source (
              num BIGINT,
              ts TIMESTAMP_LTZ(3),
              WATERMARK FOR ts AS ts
            ) WITH (
              'connector' = 'metronome'
            )""");

    assertThat(collectRows("SELECT num FROM metronome_source", 3))
        .containsExactly(Row.of(1L), Row.of(2L), Row.of(3L));
  }

  @Test
  @Timeout(30)
  void acceptsDisabledReplayOptionInSqlDdl() throws Exception {
    tEnv.executeSql(
        """
            CREATE TABLE metronome_source_no_replay (
              num BIGINT,
              ts TIMESTAMP_LTZ(3),
              WATERMARK FOR ts AS ts
            ) WITH (
              'connector' = 'metronome',
              'replay-missed-events' = 'false'
            )""");

    assertThat(collectRows("SELECT num FROM metronome_source_no_replay", 1))
        .containsExactly(Row.of(1L));
  }

  @Test
  void restoresReaderProgressWithCurrentTimestampFromCheckpointedSplit() {
    long startTimestampSec = Instant.now().getEpochSecond() - 3L;
    var reader = new MetronomeReader(unusedReaderContext(), true, 3L);
    var output = new CollectingReaderOutput();

    try {
      reader.addSplits(List.of(new MetronomeSplit(1L, startTimestampSec)));

      long beforePollSec = Instant.now().getEpochSecond();
      assertThat(reader.pollNext(output)).isEqualTo(InputStatus.MORE_AVAILABLE);
      long afterPollSec = Instant.now().getEpochSecond();

      assertThat(output.numbers()).containsExactly(2L);
      assertThat(output.eventTimestampSecs().get(0)).isBetween(beforePollSec, afterPollSec);
      assertThat(output.rowTimestampSecs().get(0)).isBetween(beforePollSec, afterPollSec);
      assertThat(reader.snapshotState(1L))
          .containsExactly(new MetronomeSplit(2L, startTimestampSec));
    } finally {
      reader.close();
    }
  }

  @Test
  void skipsMissedEventsWhenReplayIsDisabled() {
    long startTimestampSec = Instant.now().getEpochSecond() - 3L;
    var reader = new MetronomeReader(unusedReaderContext(), false, 3L);
    var output = new CollectingReaderOutput();

    try {
      reader.addSplits(List.of(new MetronomeSplit(1L, startTimestampSec)));

      long beforePollSec = Instant.now().getEpochSecond();
      assertThat(reader.pollNext(output)).isEqualTo(InputStatus.NOTHING_AVAILABLE);
      long afterPollSec = Instant.now().getEpochSecond();

      assertThat(output.numbers()).containsExactly(2L);
      assertThat(output.eventTimestampSecs().get(0)).isBetween(beforePollSec, afterPollSec);
      assertThat(output.rowTimestampSecs().get(0)).isBetween(beforePollSec, afterPollSec);

      var snapshot = reader.snapshotState(1L);
      assertThat(snapshot).hasSize(1);
      assertThat(snapshot.get(0).lastEmittedNumber()).isEqualTo(2L);
      assertThat(snapshot.get(0).startTimestampSec())
          .isBetween(beforePollSec - 2L, afterPollSec - 2L);
    } finally {
      reader.close();
    }
  }

  private List<Row> collectRows(String sql, int rowCount) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = tEnv.executeSql(sql).collect()) {
      while (rows.size() < rowCount) {
        rows.add(iterator.next());
      }
    }
    return rows;
  }

  private static SourceReaderContext unusedReaderContext() {
    return new SourceReaderContext() {
      @Override
      public SourceReaderMetricGroup metricGroup() {
        return null;
      }

      @Override
      public Configuration getConfiguration() {
        return new Configuration();
      }

      @Override
      public String getLocalHostName() {
        return "localhost";
      }

      @Override
      public int getIndexOfSubtask() {
        return 0;
      }

      @Override
      public void sendSplitRequest() {}

      @Override
      public void sendSourceEventToCoordinator(SourceEvent sourceEvent) {}

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return null;
      }
    };
  }

  private static final class CollectingReaderOutput implements ReaderOutput<RowData> {

    private final List<RowData> rows = new ArrayList<>();
    private final List<Long> eventTimestamps = new ArrayList<>();

    @Override
    public void collect(RowData record) {
      rows.add(record);
    }

    @Override
    public void collect(RowData record, long timestamp) {
      rows.add(record);
      eventTimestamps.add(timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public void markIdle() {}

    @Override
    public void markActive() {}

    @Override
    public SourceOutput<RowData> createOutputForSplit(String splitId) {
      return this;
    }

    @Override
    public void releaseOutputForSplit(String splitId) {}

    private List<Long> numbers() {
      return rows.stream().map(row -> row.getLong(0)).toList();
    }

    private List<Long> eventTimestampSecs() {
      return eventTimestamps.stream().map(timestamp -> timestamp / 1000L).toList();
    }

    private List<Long> rowTimestampSecs() {
      return rows.stream().map(row -> row.getTimestamp(1, 3).toInstant().getEpochSecond()).toList();
    }
  }
}
