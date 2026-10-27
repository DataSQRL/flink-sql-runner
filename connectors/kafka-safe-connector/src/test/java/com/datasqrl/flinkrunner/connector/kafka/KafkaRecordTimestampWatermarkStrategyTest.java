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

import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.MAX_OUT_OF_ORDERNESS_MILLIS_DEFAULT;
import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT;
import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.MIN_RECORDS_DEFAULT;
import static com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.OUT_OF_ORDERNESS_QUANTILE_DEFAULT;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.flinkrunner.connector.kafka.KafkaAdminIdleAdvanceReadinessChecker.LogEnd;
import com.datasqrl.flinkrunner.connector.kafka.SourceWatermarkOptions.SourceWatermarkConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkOutputMultiplexer;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarksContext;
import org.apache.flink.streaming.api.operators.util.PausableRelativeClock;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.watermark.WatermarkEmitStrategy;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.clock.RelativeClock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests for {@link KafkaRecordTimestampWatermarkStrategy}. */
class KafkaRecordTimestampWatermarkStrategyTest {

  private static final long IDLE_ADVANCE_TIMEOUT_MILLIS = 1_000L;
  private static final long IDLE_ADVANCE_SAFETY_MARGIN_MILLIS = 1_000L;

  /** Wall-clock time at which the job reads records written from time zero on. */
  private static final long ONE_HOUR_MILLIS = Duration.ofHours(1).toMillis();

  /** Idle advancement with a 1 s timeout and a 1 s safety margin, asking the broker every check. */
  private static final SourceWatermarkConfig IDLE_ADVANCE_CONFIG =
      idleAdvanceConfig(MIN_RECORDS_DEFAULT);

  @Test
  void testDoesNotEmitBeforeWarmup() {
    final WatermarkGenerator<RowData> generator =
        createGenerator(WatermarkEmitStrategy.ON_PERIODIC, new SourceWatermarkConfig());
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT - 1; i++) {
      generator.onEvent(null, i, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).isEmpty();
  }

  @Test
  void testEmitsWatermarkFromKafkaRecordTimestampAfterWarmup() {
    final WatermarkGenerator<RowData> generator =
        createGenerator(WatermarkEmitStrategy.ON_PERIODIC, new SourceWatermarkConfig());
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, i * 100L, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks)
        .containsExactly(
            (MIN_RECORDS_DEFAULT - 1) * 100L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1);
  }

  @Test
  void testOnEventEmitStrategyEmitsWhenWarmupCompletes() {
    final WatermarkGenerator<RowData> generator =
        createGenerator(WatermarkEmitStrategy.ON_EVENT, new SourceWatermarkConfig());
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, i * 100L, output);
    }

    assertThat(output.watermarks)
        .containsExactly(
            (MIN_RECORDS_DEFAULT - 1) * 100L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1);
  }

  @Test
  void testUsesConfiguredWarmupAndMinimumOutOfOrderness() {
    final WatermarkGenerator<RowData> generator =
        createGenerator(
            WatermarkEmitStrategy.ON_PERIODIC,
            new SourceWatermarkConfig(3, 10, 1000, 0.95D, 0, 10_000, 1000, 5000));
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    generator.onEvent(null, 0L, output);
    generator.onEvent(null, 100L, output);
    generator.onEvent(null, 200L, output);
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).containsExactly(189L);
  }

  @Test
  void testWatermarksAreMonotonic() {
    final WatermarkGenerator<RowData> generator =
        createGenerator(WatermarkEmitStrategy.ON_PERIODIC, new SourceWatermarkConfig());
    final CollectingWatermarkOutput output = new CollectingWatermarkOutput();

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, i * 100L, output);
    }
    generator.onPeriodicEmit(output);

    for (int i = 0; i < MIN_RECORDS_DEFAULT; i++) {
      generator.onEvent(null, 0L, output);
    }
    generator.onPeriodicEmit(output);

    assertThat(output.watermarks).hasSize(1);
  }

  @Test
  void testIdleAdvanceEmitsOnceIdleBeforeWarmupCompletes() {
    final Source source =
        new Source(new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 5000), 0L);
    final Split split = source.newSplit();

    split.append(1);
    split.read(10_000L);
    split.check();

    source.advanceMillis(999);
    split.check();

    source.advanceMillis(1);
    split.check();

    assertThat(split.watermarks()).containsExactly(949L);

    source.advanceMillis(10_000);
    split.check();

    assertThat(split.watermarks()).containsExactly(949L, 10_949L);
  }

  @Test
  void testIdleAdvanceDoesNotEmitWhenReadinessCheckFails() {
    final Source source =
        new Source(new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 5000), 0L);
    final Split split = source.newSplit();
    source.broker.logAppendTime = false;

    split.append(1);
    split.read(10_000L);
    split.check();
    source.advanceMillis(11_000);
    split.check();

    assertThat(split.watermarks()).isEmpty();
  }

  @Test
  void testIdleAdvanceMarksNeverSeenOutputIdleAndReactivatesOnFirstRecord() {
    final Source source =
        new Source(new SourceWatermarkConfig(250, 50, 1000, 0.95D, 1000, 10_000, 1000, 5000), 0L);
    final Split split = source.newSplit();

    source.advanceMillis(999);
    split.check();

    assertThat(split.output().idleCount).isZero();

    source.advanceMillis(1);
    split.check();

    assertThat(split.output().idleCount).isOne();
    assertThat(split.output().activeCount).isZero();
    assertThat(split.watermarks()).isEmpty();

    split.check();

    assertThat(split.output().idleCount).isOne();

    split.append(1);
    split.read(10_000L);

    assertThat(split.output().activeCount).isOne();
  }

  @Test
  void testIdleAdvanceTracksWallClockAfterCatchingUpOnBacklog() {
    // A fresh job reading history: every record is an hour old when it is read.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    split.append(10);

    // While catching up, records arrive faster than the idle-advance timeout.
    for (int i = 0; i < 10; i++) {
      split.read(i * 1_000L);
      source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS / 2);
      split.check();
    }

    assertThat(split.watermarks()).isEmpty();

    // Caught up: the split is idle from the check that saw its last record.
    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS);
    split.check();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());

    // It goes on tracking the wall clock instead of trailing it by the backlog's age.
    source.advanceMillis(Duration.ofMinutes(15).toMillis());
    split.check();

    assertThat(split.watermarks()).last().isEqualTo(source.caughtUpWatermark());
  }

  @Test
  void testIdlePartitionReadFromBacklogDoesNotHoldBackLivePartition() {
    // A source's watermark is the minimum over its partitions. One partition's only record is an
    // hour old when it is read; the other receives records in real time throughout.
    final Source source = new Source(idleAdvanceConfig(3), ONE_HOUR_MILLIS);
    final CollectingWatermarkOutput sourceOutput = new CollectingWatermarkOutput();
    final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(sourceOutput);
    final Split backlog = source.newSplit(multiplexer, "backlog");
    final Split live = source.newSplit(multiplexer, "live");

    backlog.append(1);
    backlog.read(0L);
    // One minute at Flink's default 200 ms watermark interval, with a live record every interval.
    for (int i = 0; i < 300; i++) {
      live.appendAndRead();
      source.advanceMillis(200);
      backlog.check();
      live.check();
      multiplexer.onPeriodicEmit();
    }

    assertThat(sourceOutput.idleCount).isZero();
    assertThat(sourceOutput.watermarks).last().isEqualTo(source.caughtUpWatermark());
  }

  @Test
  void testIdleAdvanceWaitsWhileRecordsAreUnread() {
    // The source is busy with other partitions or rate-limited, so this one is idle with records
    // still unread. Following the wall clock would make every one of them late.
    final Source source = new Source(idleAdvanceConfig(3), ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    split.append(10);

    split.read(0L);
    split.read(100L);
    split.read(200L);
    split.check();

    final long recordDrivenWatermark = 200L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1;
    assertThat(split.watermarks()).containsExactly(recordDrivenWatermark);

    source.advanceMillis(Duration.ofMinutes(10).toMillis());
    split.check();

    assertThat(split.watermarks()).containsExactly(recordDrivenWatermark);

    for (int i = 3; i < 10; i++) {
      split.read(i * 100L);
    }
    split.idleForTimeout();

    assertThat(split.watermarks())
        .containsExactly(
            recordDrivenWatermark,
            900L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1,
            source.caughtUpWatermark());
  }

  @Test
  void testCaughtUpPartitionDoesNotCarrySourcePastReplayingPartition() {
    // One partition has caught up and follows the wall clock. Another is still being replayed but
    // is idle, as the source is busy elsewhere. The source must stay behind its unread records.
    final Source source = new Source(idleAdvanceConfig(3), ONE_HOUR_MILLIS);
    final CollectingWatermarkOutput sourceOutput = new CollectingWatermarkOutput();
    final WatermarkOutputMultiplexer multiplexer = new WatermarkOutputMultiplexer(sourceOutput);
    final Split caughtUp = source.newSplit(multiplexer, "caught-up");
    final Split replaying = source.newSplit(multiplexer, "replaying");

    caughtUp.append(1);
    caughtUp.read(0L);
    replaying.append(10);
    replaying.read(0L);
    replaying.read(100L);
    replaying.read(200L);
    for (int i = 0; i < 50; i++) {
      source.advanceMillis(200);
      caughtUp.check();
      replaying.check();
      multiplexer.onPeriodicEmit();
    }

    final long nextUnreadTimestamp = 300L;
    assertThat(sourceOutput.watermarks)
        .last()
        .isEqualTo(200L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1);
    assertThat(sourceOutput.watermarks).allMatch(watermark -> watermark < nextUnreadTimestamp);
  }

  @Test
  void testRecordsThatProduceNoRowStillMoveReadPosition() {
    // The last records went to the dead-letter queue. They never reach the generator, but they
    // were read: the partition is idle only from a timeout after them, and caught up once they are.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    split.append(4);

    split.read(0L);
    split.check();
    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS - 1);
    split.readWithoutRow();
    split.readWithoutRow();
    split.readWithoutRow();
    split.check();
    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS - 1);
    split.check();

    assertThat(split.watermarks()).isEmpty();

    source.advanceMillis(1);
    split.check();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testLogEndFetchedBeforeLatestReadMovesWatermarkNoFurtherThanThatFetch() {
    // A fetch from before the latest read shows a log end the split has since passed, while the
    // log has grown on. It vouches only for records appended before it, so it cannot carry the
    // watermark past the records appended since, which are still unread.
    final long brokerCheckTtlMillis = 10_000L;
    final Source source =
        new Source(idleAdvanceConfig(MIN_RECORDS_DEFAULT, brokerCheckTtlMillis), ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.append(1);
    split.read(0L);
    split.idleForTimeout();

    final long firstIdleWatermark = source.caughtUpWatermark();
    assertThat(split.watermarks()).containsExactly(firstIdleWatermark);

    // Two records arrive; the split reads one a few seconds later and is then idle with the other
    // still unread.
    source.advanceMillis(3_000L);
    final long appendedMillis = source.wallClockMillis();
    split.append(2);
    source.advanceMillis(2_000L);
    split.read(appendedMillis);
    split.idleForTimeout();

    assertThat(split.watermarks()).containsExactly(firstIdleWatermark);

    // The next fetch shows the record still unread.
    source.advanceMillis(brokerCheckTtlMillis);
    split.check();

    assertThat(split.watermarks()).containsExactly(firstIdleWatermark);

    split.read(appendedMillis);
    split.check();
    source.advanceMillis(brokerCheckTtlMillis);
    split.check();

    assertThat(split.watermarks()).containsExactly(firstIdleWatermark, source.caughtUpWatermark());
  }

  @Test
  void testIdleWatermarkGoesNoFurtherThanConfirmingFetch() {
    // Records appended after the fetch may still be unread, so the watermark waits for the next.
    final long brokerCheckTtlMillis = 10_000L;
    final Source source =
        new Source(idleAdvanceConfig(MIN_RECORDS_DEFAULT, brokerCheckTtlMillis), ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.append(1);
    split.read(0L);
    split.idleForTimeout();

    final long confirmedWatermark = source.caughtUpWatermark();
    assertThat(split.watermarks()).containsExactly(confirmedWatermark);

    source.advanceMillis(brokerCheckTtlMillis - 1);
    split.check();

    assertThat(split.watermarks()).containsExactly(confirmedWatermark);

    source.advanceMillis(1);
    split.check();

    assertThat(split.watermarks()).containsExactly(confirmedWatermark, source.caughtUpWatermark());
  }

  @Test
  void testIdleAdvanceWaitsWhileLogEndIsUnknown() {
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    source.broker.logEndsAvailable = false;

    split.append(1);
    split.read(0L);
    split.check();
    source.advanceMillis(Duration.ofMinutes(10).toMillis());
    split.check();

    assertThat(split.watermarks()).isEmpty();

    source.broker.logEndsAvailable = true;
    source.advanceMillis(1);
    split.check();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testCommitMarkerAtLogEndDoesNotCountAsUnread() {
    // Each committed transaction ends in a marker, which the consumer moves past without returning
    // a record.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.beginTransaction();
    split.append(2);
    split.commitTransaction();
    split.beginTransaction();
    split.append(2);
    split.commitTransaction();

    for (int i = 0; i < 4; i++) {
      split.read(i * 100L);
    }
    split.idleForTimeout();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testOneUnreadOffsetIsRecordEvenWhereOffsetsWereSkipped() {
    // The consumer skipped the offset of a record compaction removed, but the last offset in the
    // log still holds a record: the consumer has returned it, and the split has yet to read it.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    split.append(1);
    split.appendCompactedAway();
    split.append(2);

    split.read(0L);
    split.read(100L);
    split.idleForTimeout();

    assertThat(split.watermarks()).isEmpty();

    split.read(200L);
    split.idleForTimeout();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testOpenTransactionHoldsBackReadCommittedSource() {
    // A read_committed consumer stops where the open transaction begins, so neither it nor the
    // split gets to the end of the log.
    final Source source = Source.readCommitted(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.beginTransaction();
    split.append(1);
    split.commitTransaction();
    split.append(1);
    split.read(0L);
    split.read(100L);
    split.beginTransaction();
    split.append(1);
    split.idleForTimeout();

    assertThat(split.watermarks()).isEmpty();

    // Once it commits, a read_committed consumer can read its record.
    split.commitTransaction();
    split.read(source.wallClockMillis());
    split.idleForTimeout();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testCommitMarkersOfSeveralWritersAtLogEndDoNotCountAsUnread() {
    // Writers with transactions of their own, such as the subtasks of an exactly-once sink, each
    // end theirs with a marker, so the log ends in several once they commit.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.beginTransaction();
    split.append(2);
    split.read(0L);
    split.read(100L);
    // Two writers commit.
    split.commitTransaction();
    split.commitTransaction();
    split.idleForTimeout();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testIdleAdvanceFromRecordReadAsItIsWrittenIsUnchanged() {
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    final long recordTimestamp = source.wallClockMillis();
    split.appendAndRead();
    split.idleForTimeout();

    assertThat(split.watermarks())
        .containsExactly(
            recordTimestamp
                + IDLE_ADVANCE_TIMEOUT_MILLIS
                - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT
                - IDLE_ADVANCE_SAFETY_MARGIN_MILLIS
                - 1);
  }

  @Test
  void testIdleAdvanceKeepsRecordTimestampWhenBrokerClockIsAhead() {
    // LogAppendTime comes from the broker's clock. When it runs ahead of the local wall clock, the
    // record timestamp is the later bound and idle advancement must not fall back to the wall
    // clock.
    final long brokerClockLeadMillis = 5_000L;
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.append(1);
    split.read(source.wallClockMillis() + brokerClockLeadMillis);
    split.idleForTimeout();

    assertThat(split.watermarks())
        .containsExactly(source.caughtUpWatermark() + brokerClockLeadMillis);
  }

  @ParameterizedTest
  @EnumSource(SplitPause.class)
  void testIdleAdvanceDoesNotCountPausedTimeAsIdle(SplitPause pause) {
    // A paused split stops delivering records with its backlog unread. Counting that as idle time
    // would advance its watermark past records it has not emitted yet.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    final long activeBeforePauseMillis = 400L;

    split.append(1);
    split.read(0L);
    split.check();
    source.advanceMillis(activeBeforePauseMillis);
    pause.begin(split.inputActivityClock);
    source.advanceMillis(Duration.ofMinutes(10).toMillis());
    split.check();

    assertThat(split.watermarks()).isEmpty();

    pause.end(split.inputActivityClock);
    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS - activeBeforePauseMillis - 1);
    split.check();

    assertThat(split.watermarks()).isEmpty();

    source.advanceMillis(1);
    split.check();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testIdleAdvanceDoesNotMarkSplitIdleWhileBackpressuredBeforeFirstRecord() {
    // A backpressured reader may not have read a partition yet. Marking it idle would let the
    // other partitions' watermarks run past the records it has not delivered.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    SplitPause.BACKPRESSURE.begin(split.inputActivityClock);
    source.advanceMillis(Duration.ofMinutes(10).toMillis());
    split.check();

    assertThat(split.output().idleCount).isZero();

    SplitPause.BACKPRESSURE.end(split.inputActivityClock);
    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS);
    split.check();

    assertThat(split.output().idleCount).isOne();
  }

  @Test
  void testIdleAdvanceLiftsWatermarkPastBacklogAfterWarmup() {
    // A busy partition can finish warm-up on its backlog alone, so its record-driven watermark is
    // as old as the backlog. Idle advancement still has to bring it up to date.
    final Source source = new Source(idleAdvanceConfig(3), ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    split.append(3);

    split.read(0L);
    split.read(100L);
    split.read(200L);
    split.check();

    final long recordDrivenWatermark = 200L - MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT - 1;
    assertThat(split.watermarks()).containsExactly(recordDrivenWatermark);

    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS);
    split.check();

    assertThat(split.watermarks())
        .containsExactly(recordDrivenWatermark, source.caughtUpWatermark());
  }

  @Test
  void testIdleAdvanceFromWallClockKeepsObservedOutOfOrderness() {
    // The second record arrives two seconds behind the first, so the adaptive delay is two seconds.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();
    split.append(2);

    split.read(2_000L);
    split.read(0L);
    split.idleForTimeout();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark(2_000L));
  }

  @Test
  void testRecordAfterIdleAdvanceResetsIdleTime() {
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    final long newestTimestamp = source.wallClockMillis();
    split.appendAndRead();
    split.idleForTimeout();

    final long firstIdleWatermark = source.caughtUpWatermark();
    assertThat(split.watermarks()).containsExactly(firstIdleWatermark);

    // Half a second behind the newest record, e.g. after a leader failover to a broker whose clock
    // is slightly behind. It still counts as activity.
    final long latenessMillis = 500L;
    split.append(1);
    split.read(newestTimestamp - latenessMillis);
    split.check();
    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS - 1);
    split.check();

    assertThat(split.watermarks()).containsExactly(firstIdleWatermark);

    source.advanceMillis(1);
    split.check();

    assertThat(split.watermarks())
        .containsExactly(firstIdleWatermark, source.caughtUpWatermark(latenessMillis));
  }

  @Test
  void testIdleTimeStartsAtFirstCheckAfterRecord() {
    // The input activity clock is only read at watermark checks, so a record counts as activity at
    // the first check that sees it, however long ago it arrived: idle time starts late, never
    // early.
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.append(1);
    split.read(0L);
    source.advanceMillis(5 * IDLE_ADVANCE_TIMEOUT_MILLIS);
    split.check();

    assertThat(split.watermarks()).isEmpty();

    source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS);
    split.check();

    assertThat(split.watermarks()).containsExactly(source.caughtUpWatermark());
  }

  @Test
  void testIdleAdvanceDoesNotMoveWatermarkBackWhenWallClockStepsBack() {
    final Source source = new Source(IDLE_ADVANCE_CONFIG, ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.append(1);
    split.read(0L);
    split.idleForTimeout();

    final long idleWatermark = source.caughtUpWatermark();
    assertThat(split.watermarks()).containsExactly(idleWatermark);

    // An NTP correction steps the wall clock back; the input activity clock is monotonic.
    source.stepWallClockMillis(-30_000L);
    source.advanceMillis(10_000L);
    split.check();

    assertThat(split.watermarks()).containsExactly(idleWatermark);

    source.advanceMillis(30_000L);
    split.check();

    assertThat(split.watermarks()).containsExactly(idleWatermark, source.caughtUpWatermark());
  }

  @Test
  void testDisabledIdleAdvanceDoesNotEmitForBacklog() {
    final Source source =
        new Source(
            new SourceWatermarkConfig(
                MIN_RECORDS_DEFAULT,
                MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT,
                MAX_OUT_OF_ORDERNESS_MILLIS_DEFAULT,
                OUT_OF_ORDERNESS_QUANTILE_DEFAULT,
                0L,
                IDLE_ADVANCE_SAFETY_MARGIN_MILLIS,
                1_000L,
                1L),
            ONE_HOUR_MILLIS);
    final Split split = source.newSplit();

    split.append(1);
    split.read(0L);
    source.advanceMillis(Duration.ofDays(1).toMillis());
    split.check();

    assertThat(split.watermarks()).isEmpty();
    assertThat(split.output().idleCount).isZero();
  }

  private static SourceWatermarkConfig idleAdvanceConfig(int minRecords) {
    return idleAdvanceConfig(minRecords, 1L);
  }

  private static SourceWatermarkConfig idleAdvanceConfig(
      int minRecords, long brokerCheckTtlMillis) {
    return new SourceWatermarkConfig(
        minRecords,
        MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT,
        MAX_OUT_OF_ORDERNESS_MILLIS_DEFAULT,
        OUT_OF_ORDERNESS_QUANTILE_DEFAULT,
        IDLE_ADVANCE_TIMEOUT_MILLIS,
        IDLE_ADVANCE_SAFETY_MARGIN_MILLIS,
        1_000L,
        brokerCheckTtlMillis);
  }

  /** A generator without idle advancement. */
  private static WatermarkGenerator<RowData> createGenerator(
      WatermarkEmitStrategy emitStrategy, SourceWatermarkConfig configuration) {
    return new KafkaRecordTimestampWatermarkStrategy(emitStrategy, configuration, null)
        .createWatermarkGenerator(context(SystemClock.getInstance()));
  }

  /** The context the source operator hands each split's generator. */
  private static WatermarkGeneratorSupplier.Context context(RelativeClock inputActivityClock) {
    return new TimestampsAndWatermarksContext(new UnregisteredMetricsGroup(), inputActivityClock);
  }

  /** The two ways Flink stops a split's input activity clock. */
  enum SplitPause {
    /** Watermark alignment, via {@code ProgressiveTimestampsAndWatermarks#pauseOrResumeSplits}. */
    WATERMARK_ALIGNMENT {
      @Override
      void begin(PausableRelativeClock clock) {
        clock.pause();
      }

      @Override
      void end(PausableRelativeClock clock) {
        clock.unPause();
      }
    },

    /**
     * Back pressure, via the task's back-pressure timer gauge that every split clock listens to.
     */
    BACKPRESSURE {
      @Override
      void begin(PausableRelativeClock clock) {
        clock.markStart();
      }

      @Override
      void end(PausableRelativeClock clock) {
        clock.markEnd();
      }
    };

    abstract void begin(PausableRelativeClock clock);

    abstract void end(PausableRelativeClock clock);
  }

  /**
   * A source subtask: the broker its splits read from, and the clocks they see: the wall clock,
   * which NTP may step, and the monotonic time behind each split's input activity clock.
   */
  private static final class Source {

    private final SourceWatermarkConfig config;
    private final FakeBroker broker;
    private final ManualClock monotonicClock = new ManualClock();
    private long wallClockMillis;

    /** Whether the source's consumer stops where the first open transaction begins. */
    private final boolean readCommitted;

    /** The interceptor on the source's consumer, which publishes how far it has fetched. */
    private final KafkaFetchPositions fetchPositions = new KafkaFetchPositions();

    private Source(SourceWatermarkConfig config, long wallClockMillis) {
      this(config, wallClockMillis, false);
    }

    private Source(SourceWatermarkConfig config, long wallClockMillis, boolean readCommitted) {
      String sourceId = UUID.randomUUID().toString();
      this.config = config;
      this.broker = new FakeBroker(config, sourceId);
      this.wallClockMillis = wallClockMillis;
      this.readCommitted = readCommitted;
      fetchPositions.configure(Map.of(KafkaFetchPositions.SOURCE_ID_CONFIG, sourceId));
    }

    private static Source readCommitted(SourceWatermarkConfig config, long wallClockMillis) {
      return new Source(config, wallClockMillis, true);
    }

    /** A split whose watermarks go to an output of its own. */
    private Split newSplit() {
      return new Split(this, new CollectingWatermarkOutput());
    }

    /** A split whose watermarks the multiplexer combines with those of the source's others. */
    private Split newSplit(WatermarkOutputMultiplexer multiplexer, String splitId) {
      multiplexer.registerNewOutput(splitId);
      return new Split(this, multiplexer.getDeferredOutput(splitId));
    }

    private void advanceMillis(long millis) {
      wallClockMillis += millis;
      monotonicClock.advanceTime(millis, TimeUnit.MILLISECONDS);
    }

    /** Moves only the wall clock, as an NTP correction does. */
    private void stepWallClockMillis(long millis) {
      wallClockMillis += millis;
    }

    private long wallClockMillis() {
      return wallClockMillis;
    }

    /** The idle watermark of a split that a log-end fetch issued now shows caught up. */
    private long caughtUpWatermark() {
      return caughtUpWatermark(MIN_OUT_OF_ORDERNESS_MILLIS_DEFAULT);
    }

    private long caughtUpWatermark(long outOfOrdernessMillis) {
      return wallClockMillis - outOfOrdernessMillis - IDLE_ADVANCE_SAFETY_MARGIN_MILLIS - 1;
    }
  }

  /** One partition's log on the broker, and the split that reads it. */
  private static final class Split {

    private final Source source;
    private final TopicPartition partition = new TopicPartition("topic-" + UUID.randomUUID(), 0);
    private final PausableRelativeClock inputActivityClock;
    private final WatermarkOutput output;
    private final WatermarkGenerator<RowData> generator;

    /**
     * One entry per offset in the log: whether the consumer skips it, as it does transaction
     * markers and the offsets of records compaction removed.
     */
    private final List<Boolean> markers = new ArrayList<>();

    /** The offset at which the open transaction began, or -1 if none is open. */
    private long openTransactionStart = -1L;

    /** The offset the source's consumer fetches next. */
    private long fetchOffset;

    /** The offset the split reads next. */
    private long readOffset;

    private Split(Source source, WatermarkOutput output) {
      this.source = source;
      this.inputActivityClock = new PausableRelativeClock(source.monotonicClock);
      this.output = output;
      this.generator =
          new KafkaRecordTimestampWatermarkStrategy(
                  WatermarkEmitStrategy.ON_PERIODIC,
                  source.config,
                  source::wallClockMillis,
                  source.broker)
              .createWatermarkGenerator(context(inputActivityClock));
      source.broker.logs.put(partition, this);
    }

    /** Appends records the split has not read yet. */
    private void append(int records) {
      for (int i = 0; i < records; i++) {
        markers.add(false);
      }
    }

    /** Appends the offset of a record that compaction has since removed. */
    private void appendCompactedAway() {
      markers.add(true);
    }

    private void beginTransaction() {
      openTransactionStart = markers.size();
    }

    private void commitTransaction() {
      markers.add(true);
      openTransactionStart = -1L;
    }

    private long highWatermark() {
      return markers.size();
    }

    /** Where a read_committed consumer has to stop. */
    private long lastStableOffset() {
      return openTransactionStart >= 0 ? openTransactionStart : highWatermark();
    }

    /** Reads the next record, skipping markers as the consumer does; its timestamp is given. */
    private void read(long timestamp) {
      consumeNext();
      generator.onEvent(null, timestamp, output);
    }

    /** Reads the next record, which produces no row, e.g. one sent to the dead-letter queue. */
    private void readWithoutRow() {
      consumeNext();
    }

    /** Appends a record and reads it at once, as a source that keeps up does. */
    private void appendAndRead() {
      append(1);
      read(source.wallClockMillis());
    }

    /** A periodic watermark check, after the consumer has fetched in the background. */
    private void check() {
      fetch();
      generator.onPeriodicEmit(output);
    }

    /** The check that sees the latest reads, and the one an idle-advance timeout later. */
    private void idleForTimeout() {
      check();
      source.advanceMillis(IDLE_ADVANCE_TIMEOUT_MILLIS);
      check();
    }

    private CollectingWatermarkOutput output() {
      return (CollectingWatermarkOutput) output;
    }

    private List<Long> watermarks() {
      return output().watermarks;
    }

    /** What the deserializer does with the next record, before its rows reach the generator. */
    private void consumeNext() {
      while (readOffset < markers.size() && markers.get((int) readOffset)) {
        readOffset++;
      }
      checkState(readOffset < markers.size(), "No record left to read at offset %s", readOffset);
      if (readOffset >= fetchOffset) {
        fetch();
      }
      KafkaReadPositions.consumed(partition.topic(), partition.partition(), readOffset++);
    }

    /**
     * A poll of the source's consumer, ahead of the split's reads: it fetches all it may read,
     * returning the records and moving past the offsets it skips, and hands both to its
     * interceptor.
     */
    private void fetch() {
      long readableEnd = source.readCommitted ? lastStableOffset() : highWatermark();
      if (fetchOffset >= readableEnd) {
        // A poll that moves nowhere returns without calling the interceptors.
        return;
      }

      var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
      for (long offset = fetchOffset; offset < readableEnd; offset++) {
        if (!markers.get((int) offset)) {
          records.add(
              new ConsumerRecord<>(partition.topic(), partition.partition(), offset, null, null));
        }
      }
      source.fetchPositions.onConsume(
          new ConsumerRecords<>(
              records.isEmpty() ? Map.of() : Map.of(partition, records),
              Map.of(partition, new OffsetAndMetadata(readableEnd))));
      fetchOffset = readableEnd;
    }
  }

  /** The broker as idle advancement asks it: whether topics use LogAppendTime, and log ends. */
  private static final class FakeBroker extends KafkaAdminIdleAdvanceReadinessChecker {

    private final Map<TopicPartition, Split> logs = new HashMap<>();
    private boolean logAppendTime = true;
    private boolean logEndsAvailable = true;

    private FakeBroker(SourceWatermarkConfig config, String sourceId) {
      super(KafkaFetchPositions.publishing(new Properties(), sourceId), List.of("topic"), config);
    }

    @Override
    boolean checkBrokerAndTopicTimestampType() {
      return logAppendTime;
    }

    @Override
    @Nullable
    Map<TopicPartition, LogEnd> listLogEnds(
        Set<TopicPartition> partitions, long requestedAtMillis) {
      if (!logEndsAvailable) {
        return null;
      }

      var logEnds = new HashMap<TopicPartition, LogEnd>();
      for (TopicPartition partition : partitions) {
        logEnds.put(partition, new LogEnd(requestedAtMillis, logs.get(partition).highWatermark()));
      }

      return logEnds;
    }
  }

  private static final class CollectingWatermarkOutput implements WatermarkOutput {

    private final List<Long> watermarks = new ArrayList<>();
    private int idleCount;
    private int activeCount;

    @Override
    public void emitWatermark(Watermark watermark) {
      watermarks.add(watermark.getTimestamp());
    }

    @Override
    public void markIdle() {
      idleCount++;
    }

    @Override
    public void markActive() {
      activeCount++;
    }
  }
}
