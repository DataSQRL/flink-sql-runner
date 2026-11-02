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
package com.datasqrl.flinkrunner.connector.kafka;

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

/**
 * Records in {@link KafkaReadPositions} the partition and starting offset of every split assigned
 * to the reader. A watermark generator learns its split only from its first row, so idle
 * advancement needs these to tell whether the splits that have not produced a row have anything
 * left to read. Flink assigns splits on the source task's thread, whose positions the generators
 * read.
 */
@Internal
public final class AssignmentRecordingSource<T, EnumChkT>
    extends ForwardingSource<T, KafkaPartitionSplit, EnumChkT> {

  public AssignmentRecordingSource(Source<T, KafkaPartitionSplit, EnumChkT> delegate) {
    super(delegate);
  }

  @Override
  public SourceReader<T, KafkaPartitionSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return new AssignmentRecordingReader<>(delegate.createReader(readerContext));
  }

  private static final class AssignmentRecordingReader<T>
      extends ForwardingReader<T, KafkaPartitionSplit> {

    private AssignmentRecordingReader(SourceReader<T, KafkaPartitionSplit> delegate) {
      super(delegate);
    }

    @Override
    public void addSplits(List<KafkaPartitionSplit> splits) {
      splits.forEach(
          split ->
              KafkaReadPositions.assigned(split.getTopicPartition(), split.getStartingOffset()));
      super.addSplits(splits);
    }
  }
}
