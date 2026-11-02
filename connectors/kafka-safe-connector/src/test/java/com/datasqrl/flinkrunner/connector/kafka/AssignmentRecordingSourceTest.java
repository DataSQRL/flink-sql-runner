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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AssignmentRecordingSourceTest {

  private static final TopicPartition PARTITION = new TopicPartition("topic", 3);

  @SuppressWarnings("unchecked")
  private final SourceReader<String, KafkaPartitionSplit> reader = mock(SourceReader.class);

  @AfterEach
  void clearReadPositions() {
    KafkaReadPositions.clear();
  }

  @Test
  void testRecordsAssignedSplitsBeforeTheReaderGetsThem() throws Exception {
    var assignmentsWhenReaderGotSplits = new ArrayList<List<ReadPosition>>();
    doAnswer(
            invocation -> {
              assignmentsWhenReaderGotSplits.add(KafkaReadPositions.assignments());
              return null;
            })
        .when(reader)
        .addSplits(anyList());

    recordingReader().addSplits(List.of(new KafkaPartitionSplit(PARTITION, 42L)));

    assertThat(assignmentsWhenReaderGotSplits)
        .singleElement()
        .satisfies(
            assignments ->
                assertThat(assignments)
                    .extracting(ReadPosition::partition, ReadPosition::startingOffset)
                    .containsExactly(tuple(PARTITION, 42L)));
  }

  @Test
  void testForwardsReaderCallbacks() throws Exception {
    // KafkaSourceReader commits offsets on checkpoint completion; alignment pauses splits.
    var recordingReader = recordingReader();

    recordingReader.snapshotState(7L);
    recordingReader.notifyCheckpointComplete(7L);
    recordingReader.notifyCheckpointAborted(8L);
    recordingReader.pauseOrResumeSplits(List.of("paused"), List.of("resumed"));
    recordingReader.close();

    verify(reader).snapshotState(7L);
    verify(reader).notifyCheckpointComplete(7L);
    verify(reader).notifyCheckpointAborted(8L);
    verify(reader).pauseOrResumeSplits(List.of("paused"), List.of("resumed"));
    verify(reader).close();
  }

  @SuppressWarnings("unchecked")
  private SourceReader<String, KafkaPartitionSplit> recordingReader() throws Exception {
    Source<String, KafkaPartitionSplit, Void> source = mock(Source.class);
    when(source.createReader(any())).thenReturn(reader);
    return new AssignmentRecordingSource<>(source).createReader(mock(SourceReaderContext.class));
  }
}
