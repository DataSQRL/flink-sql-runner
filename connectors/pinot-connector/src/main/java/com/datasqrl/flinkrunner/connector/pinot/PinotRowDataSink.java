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
package com.datasqrl.flinkrunner.connector.pinot;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.table.data.RowData;
import org.apache.pinot.connector.flink.sink.FlinkSegmentWriter;
import org.apache.pinot.plugin.segmentuploader.SegmentUploaderDefault;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.segment.uploader.SegmentUploader;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 2.x {@link Sink} that writes {@link RowData} records into Apache Pinot OFFLINE segments.
 *
 * <p>Mirrors the logic of {@code PinotSink} from pinot-flink-connector (available in Pinot 1.6.0+),
 * implemented here against the 1.3.0 APIs so no SNAPSHOT dependency is required.
 *
 * <p><b>Delivery guarantee:</b> at-least-once. Segments are uploaded on {@code flush()} (called at
 * every Flink checkpoint) and when the row buffer reaches {@code segmentFlushRows}. A task restart
 * between upload and checkpoint acknowledgement may re-send rows; enable Pinot deduplication if
 * exactly-once is required.
 */
@RequiredArgsConstructor
public class PinotRowDataSink implements Sink<RowData> {

  private static final int EXECUTOR_POOL_SIZE = 5;
  private static final long EXECUTOR_SHUTDOWN_WAIT_MS = 3_000;

  private final TableConfig tableConfig;
  private final Schema schema;
  private final long segmentFlushRows;
  private final PinotRowDataConverter converter;

  @Override
  public SinkWriter<RowData> createWriter(WriterInitContext context) throws IOException {
    return new Writer(context);
  }

  private final class Writer implements SinkWriter<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(Writer.class);

    private final SegmentWriter segmentWriter;
    private final SegmentUploader segmentUploader;
    private final ExecutorService executor;
    private final List<Future<?>> pendingUploads = new ArrayList<>();
    private long rowCount;

    private Writer(WriterInitContext context) throws IOException {
      executor = Executors.newFixedThreadPool(EXECUTOR_POOL_SIZE);
      SegmentWriter sw = null;
      SegmentUploader su = null;
      try {
        sw =
            new FlinkSegmentWriter(
                context.getTaskInfo().getIndexOfThisSubtask(), context.metricGroup());
        sw.init(tableConfig, schema);
        su = new SegmentUploaderDefault();
        su.init(tableConfig);
        segmentWriter = sw;
        segmentUploader = su;
        LOG.info("Opened Pinot sink for table {}", tableConfig.getTableName());
      } catch (Exception e) {
        executor.shutdownNow();
        closeQuietly(sw);
        throw new IOException("Failed to initialise Pinot sink writer", e);
      }
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
      drainCompleted();
      try {
        segmentWriter.collect(converter.convert(element));
      } catch (Exception e) {
        throw new IOException("Failed to collect row into Pinot segment buffer", e);
      }
      if (++rowCount >= segmentFlushRows) {
        flushAsync();
      }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
      if (rowCount > 0) {
        flushAsync();
      }
      awaitAllUploads();
    }

    @Override
    public void close() throws Exception {
      Exception failure = null;
      try {
        flush(true);
      } catch (Exception e) {
        restoreInterrupt(e);
        failure = e;
      } finally {
        try {
          shutdownExecutor();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (failure == null) failure = e;
          else failure.addSuppressed(e);
        }
        try {
          segmentWriter.close();
        } catch (Exception e) {
          if (failure == null) failure = e;
          else failure.addSuppressed(e);
        }
      }
      if (failure != null) throw failure;
    }

    private void flushAsync() throws IOException {
      final URI segmentUri;
      try {
        segmentUri = segmentWriter.flush();
      } catch (Exception e) {
        throw new IOException("Failed to flush Pinot segment", e);
      }
      rowCount = 0;
      pendingUploads.add(
          executor.submit(
              () -> {
                try {
                  segmentUploader.uploadSegment(segmentUri, null);
                } catch (Exception e) {
                  throw new RuntimeException("Failed to upload Pinot segment " + segmentUri, e);
                }
              }));
    }

    private void drainCompleted() throws IOException, InterruptedException {
      Iterator<Future<?>> it = pendingUploads.iterator();
      while (it.hasNext()) {
        Future<?> f = it.next();
        if (f.isDone()) {
          await(f);
          it.remove();
        }
      }
    }

    private void awaitAllUploads() throws IOException, InterruptedException {
      for (Future<?> f : pendingUploads) {
        await(f);
      }
      pendingUploads.clear();
    }

    private void await(Future<?> f) throws IOException, InterruptedException {
      try {
        f.get();
      } catch (ExecutionException e) {
        Throwable cause =
            e.getCause() instanceof RuntimeException && e.getCause().getCause() != null
                ? e.getCause().getCause()
                : e.getCause();
        if (cause instanceof IOException) throw (IOException) cause;
        if (cause instanceof InterruptedException) {
          Thread.currentThread().interrupt();
          throw (InterruptedException) cause;
        }
        throw new IOException("Pinot segment upload failed", cause);
      }
    }

    private void shutdownExecutor() throws InterruptedException {
      executor.shutdown();
      if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    }

    private void restoreInterrupt(Exception e) {
      if (e instanceof InterruptedException) Thread.currentThread().interrupt();
    }

    private void closeQuietly(SegmentWriter sw) {
      if (sw == null) return;
      try {
        sw.close();
      } catch (Exception ignored) {
      }
    }
  }
}
