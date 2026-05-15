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

import com.datasqrl.flinkrunner.connector.datagen.metronome.enumerator.MetronomeEnumerator;
import com.datasqrl.flinkrunner.connector.datagen.metronome.enumerator.MetronomeEnumeratorState;
import com.datasqrl.flinkrunner.connector.datagen.metronome.enumerator.MetronomeEnumeratorStateSerializer;
import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplit;
import com.datasqrl.flinkrunner.connector.datagen.metronome.split.MetronomeSplitSerializer;
import java.io.Serial;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

/** Source that produces a single global metronome sequence with event-time timestamps. */
public class MetronomeSource implements Source<RowData, MetronomeSplit, MetronomeEnumeratorState> {

  @Serial private static final long serialVersionUID = 1L;

  @Nullable private final Long numberOfRows;

  public MetronomeSource() {
    this(null);
  }

  public MetronomeSource(@Nullable Long numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  @Override
  public Boundedness getBoundedness() {
    return numberOfRows == null ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<RowData, MetronomeSplit> createReader(SourceReaderContext readerContext) {
    return new MetronomeReader(readerContext, numberOfRows);
  }

  @Override
  public SplitEnumerator<MetronomeSplit, MetronomeEnumeratorState> createEnumerator(
      SplitEnumeratorContext<MetronomeSplit> enumContext) {

    return new MetronomeEnumerator(enumContext, MetronomeEnumeratorState.unassigned());
  }

  @Override
  public SplitEnumerator<MetronomeSplit, MetronomeEnumeratorState> restoreEnumerator(
      SplitEnumeratorContext<MetronomeSplit> enumContext,
      MetronomeEnumeratorState checkpointState) {

    return new MetronomeEnumerator(enumContext, checkpointState);
  }

  @Override
  public SimpleVersionedSerializer<MetronomeSplit> getSplitSerializer() {
    return new MetronomeSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<MetronomeEnumeratorState> getEnumeratorCheckpointSerializer() {
    return new MetronomeEnumeratorStateSerializer();
  }
}
