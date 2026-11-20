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
package com.datasqrl.flinkrunner.connector.camel.source.cursor;

import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import com.datasqrl.flinkrunner.connector.camel.source.ContractTypes;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import com.datasqrl.flinkrunner.connector.camel.source.SplitListSerializer;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollSettings;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollingSourceBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.InstantiationUtil;

/**
 * A Flink source that reads a {@link CursorSource} in cursor mode: it asks the system for
 * everything since the last position and keeps that position in Flink state, so a restore resumes
 * extraction without gaps. Boundary records may be replayed and are deduplicated downstream.
 *
 * <pre>{@code
 * CamelCursorSource<Account, Instant> source = CamelCursorSource.builder(new Accounts())
 *         .pollInterval(Duration.ofMinutes(5))
 *         .build();
 *
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "accounts");
 * }</pre>
 *
 * <p>Every record of one poll carries the poll's timestamp. With {@link
 * Builder#pollWatermarks(boolean)} a watermark at that timestamp follows a completed poll that
 * emitted rows, meaning the source was caught up as of then. With {@link
 * Builder#fullRefresh(boolean)} every poll reads everything from the start position: a snapshot for
 * downstream reconciliation.
 *
 * <p>The source has one split, so it reads on a single subtask regardless of parallelism.
 */
@Internal
public final class CamelCursorSource<R, POS>
    implements Source<R, CursorSplit<POS>, List<CursorSplit<POS>>>, ResultTypeQueryable<R> {

  private final CursorSettings<?, POS, ?, R> settings;
  private final TypeInformation<R> producedType;

  private CamelCursorSource(
      CursorSettings<?, POS, ?, R> settings, TypeInformation<R> producedType) {
    this.settings = settings;
    this.producedType = producedType;
  }

  /** Emits what the source maps, typed by {@link CursorSource}'s first type argument. */
  public static <OUT, POS, IN> Builder<OUT, POS, IN, OUT> builder(
      CursorSource<OUT, POS, IN> source) {
    return new Builder<>(
        source, RecordConverter.identity(), ContractTypes.argument(CursorSource.class, source, 0));
  }

  /** Emits what {@code converter} makes of each mapped record. */
  public static <OUT, POS, IN, R> Builder<OUT, POS, IN, R> builder(
      CursorSource<OUT, POS, IN> source,
      RecordConverter<OUT, R> converter,
      TypeInformation<R> producedType) {
    return new Builder<>(source, converter, producedType);
  }

  @Override
  public Boundedness getBoundedness() {
    return settings.poll().bounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<R, CursorSplit<POS>> createReader(SourceReaderContext readerContext)
      throws Exception {
    return createReader(settings, readerContext);
  }

  private static <OUT, POS, IN, R> SourceReader<R, CursorSplit<POS>> createReader(
      CursorSettings<OUT, POS, IN, R> settings, SourceReaderContext readerContext)
      throws Exception {
    settings.converter().open(readerContext);
    CamelRuntime runtime =
        new CamelRuntime(
            readerContext.getUserCodeClassLoader().asClassLoader(),
            settings.source(),
            settings.componentOptions(),
            null,
            settings.poll().newPollErrors(settings.source().getClass().getName()),
            endpoint -> {});
    Counter dropped = readerContext.metricGroup().counter("numRecordsDropped");
    Counter failed = readerContext.metricGroup().counter("numRecordsFailed");
    return new CursorSourceReader<>(
        () -> new CursorSplitReader<>(settings, runtime, dropped, failed),
        settings.poll().pollWatermarks(),
        runtime,
        readerContext);
  }

  @Override
  public SplitEnumerator<CursorSplit<POS>, List<CursorSplit<POS>>> createEnumerator(
      SplitEnumeratorContext<CursorSplit<POS>> enumContext) {
    return new CamelSplitEnumerator<>(enumContext, List.of(CursorSplit.initial()));
  }

  @Override
  public SplitEnumerator<CursorSplit<POS>, List<CursorSplit<POS>>> restoreEnumerator(
      SplitEnumeratorContext<CursorSplit<POS>> enumContext, List<CursorSplit<POS>> unassigned) {
    return new CamelSplitEnumerator<>(enumContext, unassigned);
  }

  @Override
  public SimpleVersionedSerializer<CursorSplit<POS>> getSplitSerializer() {
    return new CursorSplitSerializer<>(settings.positionSerializer());
  }

  @Override
  public SimpleVersionedSerializer<List<CursorSplit<POS>>> getEnumeratorCheckpointSerializer() {
    return new SplitListSerializer<>(getSplitSerializer());
  }

  @Override
  public TypeInformation<R> getProducedType() {
    return producedType;
  }

  /**
   * Builds a {@link CamelCursorSource}. The position type is extracted from the {@link
   * CursorSource}'s type arguments when not set.
   */
  @Internal
  public static final class Builder<OUT, POS, IN, R>
      extends PollingSourceBuilder<Builder<OUT, POS, IN, R>> {

    private final CursorSource<OUT, POS, IN> source;
    private final RecordConverter<OUT, R> converter;
    private final TypeInformation<R> producedType;
    private TypeInformation<POS> positionType;
    private boolean fullRefresh;
    private Duration lookback = Duration.ZERO;
    private POS startPosition;

    private Builder(
        CursorSource<OUT, POS, IN> source,
        RecordConverter<OUT, R> converter,
        TypeInformation<R> producedType) {
      this.source = Objects.requireNonNull(source, "source");
      this.converter = Objects.requireNonNull(converter, "converter");
      this.producedType = Objects.requireNonNull(producedType, "producedType");
    }

    /** The type of the position, which determines how it is written to state. */
    public Builder<OUT, POS, IN, R> positionType(TypeInformation<POS> positionType) {
      this.positionType = positionType;
      return this;
    }

    /**
     * Full refresh ({@code cursor.reset = per-poll}): every poll pages through everything from the
     * start position instead of continuing from the last one. Off by default.
     */
    public Builder<OUT, POS, IN, R> fullRefresh(boolean fullRefresh) {
      this.fullRefresh = fullRefresh;
      return this;
    }

    /**
     * Starts every request this far before the position ({@code cursor.lookback}), to read rows
     * that concurrent writers committed after rows with a later position were already read. Rows in
     * the window are emitted again and deduplicated downstream. The position type must be {@link
     * Comparable} and supported by {@link CursorSource#rewind}. Off by default.
     */
    public Builder<OUT, POS, IN, R> lookback(Duration lookback) {
      this.lookback = lookback;
      return this;
    }

    /**
     * Starts from this position instead of {@link CursorSource#initial()} ({@code cursor.start}). A
     * restored position takes precedence.
     */
    public Builder<OUT, POS, IN, R> startPosition(POS startPosition) {
      this.startPosition = startPosition;
      return this;
    }

    public CamelCursorSource<R, POS> build() {
      PollSettings poll = pollSettings();
      TypeInformation<POS> posType =
          positionType != null
              ? positionType
              : ContractTypes.argument(CursorSource.class, source, 1);
      checkLookback(posType);
      TypeSerializer<POS> positionSerializer = posType.createSerializer(new SerializerConfigImpl());
      byte[] start;
      try {
        start =
            startPosition == null
                ? null
                : InstantiationUtil.serializeToByteArray(positionSerializer, startPosition);
      } catch (IOException e) {
        throw new IllegalArgumentException("Cannot serialize the start position", e);
      }
      return new CamelCursorSource<>(
          new CursorSettings<>(
              source,
              converter,
              componentOptions(),
              poll,
              fullRefresh,
              lookback,
              positionSerializer,
              start),
          producedType);
    }

    private void checkLookback(TypeInformation<POS> posType) {
      if (lookback == null || lookback.isNegative()) {
        throw new IllegalArgumentException("lookback must not be negative, got " + lookback);
      }
      if (lookback.isZero()) {
        return;
      }
      if (fullRefresh) {
        throw new IllegalArgumentException(
            "lookback does not apply to a full refresh, which re-reads everything");
      }
      // Re-read records may only keep the position where it is, so positions are compared.
      if (!Comparable.class.isAssignableFrom(posType.getTypeClass())) {
        throw new IllegalArgumentException("lookback needs a Comparable position, got " + posType);
      }
      POS start = startPosition != null ? startPosition : source.initial();
      try {
        source.rewind(start, lookback);
      } catch (RuntimeException e) {
        throw new IllegalArgumentException(
            "lookback cannot rewind the position " + start + " of " + source.getClass().getName(),
            e);
      }
    }
  }
}
