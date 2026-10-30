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
import static org.assertj.core.groups.Tuple.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.datasqrl.flinkrunner.connector.kafka.AssignmentTrackingSource.AssignmentTrackingReader;
import com.datasqrl.flinkrunner.connector.kafka.KafkaReadPositions.ReadPosition;
import java.util.List;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class AssignmentTrackingSourceTest {

  @AfterEach
  void clearReadPositions() {
    KafkaReadPositions.clear();
  }

  @Test
  void testRecordsAssignedSplitsBeforeHandingThemToKafkaReader() throws Exception {
    @SuppressWarnings("unchecked")
    SourceReader<String, KafkaPartitionSplit> delegate = mock(SourceReader.class);
    var reader = new AssignmentTrackingReader<>(delegate);
    var restored = new KafkaPartitionSplit(new TopicPartition("topic", 0), 42L);
    var fresh =
        new KafkaPartitionSplit(
            new TopicPartition("topic", 1), KafkaPartitionSplit.COMMITTED_OFFSET);

    reader.addSplits(List.of(restored, fresh));

    assertThat(KafkaReadPositions.assignments())
        .extracting(ReadPosition::partition, ReadPosition::startingOffset)
        .containsExactlyInAnyOrder(
            tuple(new TopicPartition("topic", 0), 42L),
            tuple(new TopicPartition("topic", 1), KafkaPartitionSplit.COMMITTED_OFFSET));
    verify(delegate).addSplits(List.of(restored, fresh));
  }

  @Test
  void testForwardsOffsetCommitsAndAlignmentPauses() throws Exception {
    @SuppressWarnings("unchecked")
    SourceReader<String, KafkaPartitionSplit> delegate = mock(SourceReader.class);
    var reader = new AssignmentTrackingReader<>(delegate);

    reader.notifyCheckpointComplete(7L);
    reader.pauseOrResumeSplits(List.of("topic-0"), List.of("topic-1"));

    verify(delegate).notifyCheckpointComplete(7L);
    verify(delegate).pauseOrResumeSplits(List.of("topic-0"), List.of("topic-1"));
  }
}
