/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkAlignmentParams;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.mocks.MockSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumerator;
import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.ReportedWatermarkEvent;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

class SourceCoordinatorAlignmentFailoverTest extends SourceCoordinatorTestBase {

    private static final WatermarkAlignmentParams ALIGNMENT =
            new WatermarkAlignmentParams(1000L, "group", Long.MAX_VALUE);

    private final CoordinatorStore coordinatorStore = new CoordinatorStoreImpl();

    @Test
    void recreatedCoordinatorDropsWatermarkOfIdleSource() throws Exception {
        try (var closeables = new AutoCloseableRegistry()) {
            var sparse = startCoordinator("sparse", closeables);
            var busy = startCoordinator("busy", closeables);
            reportWatermark(sparse, 0, 42);
            reportWatermark(busy, 0, 5000);
            assertLatestMaxAllowedWatermark(0, 1042);

            sparse.close();
            startCoordinator("sparse", closeables);
            reportWatermark(busy, 0, 5001);

            assertLatestMaxAllowedWatermark(0, 6001);
        }
    }

    @Test
    void resetSubtaskDropsItsWatermark() throws Exception {
        try (var closeables = new AutoCloseableRegistry()) {
            var coordinator = startCoordinator("source", closeables);
            reportWatermark(coordinator, 0, 42);
            reportWatermark(coordinator, 1, 5000);
            assertLatestMaxAllowedWatermark(1, 1042);

            coordinator.subtaskReset(0, 1L);
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
            coordinator.announceCombinedWatermark();

            assertLatestMaxAllowedWatermark(1, 6000);
        }
    }

    @Test
    void reportProcessedWhileClosingDoesNotRestoreWatermark() throws Exception {
        try (var closeables = new AutoCloseableRegistry()) {
            var enumeratorClosing = new CountDownLatch(1);
            var releaseEnumeratorClose = new CountDownLatch(1);
            var sparse =
                    startCoordinator(
                            "sparse",
                            new SlowClosingSource(enumeratorClosing, releaseEnumeratorClose),
                            closeables);
            var busy = startCoordinator("busy", closeables);
            reportWatermark(sparse, 0, 42);
            reportWatermark(busy, 0, 5000);
            assertLatestMaxAllowedWatermark(0, 1042);

            // A report from before the failover is still queued on the coordinator thread.
            var releaseCoordinatorThread = new CountDownLatch(1);
            sparse.getContext()
                    .runInCoordinatorThread(
                            ThrowingRunnable.unchecked(releaseCoordinatorThread::await));
            sparse.handleEventFromOperator(0, 0, new ReportedWatermarkEvent(100));

            var closing = CompletableFuture.runAsync(ThrowingRunnable.unchecked(sparse::close));
            // close() is now in enumerator.close(); let the queued report run meanwhile.
            enumeratorClosing.await();
            releaseCoordinatorThread.countDown();
            CoordinatorTestUtils.waitForCoordinatorToProcessActions(sparse.getContext());
            releaseEnumeratorClose.countDown();
            closing.get();

            startCoordinator("sparse", closeables);
            reportWatermark(busy, 0, 5001);

            assertLatestMaxAllowedWatermark(0, 6001);
        }
    }

    private SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> startCoordinator(
            String operatorName, AutoCloseableRegistry closeables) throws Exception {
        return startCoordinator(operatorName, createMockSource(), closeables);
    }

    private SourceCoordinator<MockSourceSplit, Set<MockSourceSplit>> startCoordinator(
            String operatorName,
            Source<Integer, MockSourceSplit, Set<MockSourceSplit>> source,
            AutoCloseableRegistry closeables)
            throws Exception {
        var coordinator =
                new SourceCoordinator<>(
                        new JobID(),
                        operatorName,
                        source,
                        getNewSourceCoordinatorContext(),
                        coordinatorStore,
                        ALIGNMENT,
                        null);
        closeables.registerCloseable(coordinator);
        coordinator.start();
        setAllReaderTasksReady(coordinator);
        return coordinator;
    }

    private void reportWatermark(SourceCoordinator<?, ?> coordinator, int subtask, long watermark) {
        coordinator.handleEventFromOperator(subtask, 0, new ReportedWatermarkEvent(watermark));
        CoordinatorTestUtils.waitForCoordinatorToProcessActions(coordinator.getContext());
        coordinator.announceCombinedWatermark();
    }

    private void assertLatestMaxAllowedWatermark(int subtask, long expected) {
        List<OperatorEvent> events = receivingTasks.getSentEventsForSubtask(subtask);
        assertThat(events).last().isEqualTo(new WatermarkAlignmentEvent(expected));
    }

    /** A source whose enumerator takes a while to close, like one closing a remote client. */
    private static class SlowClosingSource extends MockSource {

        private final CountDownLatch closing;
        private final CountDownLatch release;

        SlowClosingSource(CountDownLatch closing, CountDownLatch release) {
            super(Boundedness.CONTINUOUS_UNBOUNDED, 2);
            this.closing = closing;
            this.release = release;
        }

        @Override
        public SplitEnumerator<MockSourceSplit, Set<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {
            return new MockSplitEnumerator(2, enumContext) {
                @Override
                public void close() throws IOException {
                    closing.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    super.close();
                }
            };
        }
    }
}
