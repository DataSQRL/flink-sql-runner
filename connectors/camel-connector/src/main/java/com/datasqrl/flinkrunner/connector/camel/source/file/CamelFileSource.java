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
package com.datasqrl.flinkrunner.connector.camel.source.file;

import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import com.datasqrl.flinkrunner.connector.camel.source.ContractTypes;
import com.datasqrl.flinkrunner.connector.camel.source.PollErrors;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import com.datasqrl.flinkrunner.connector.camel.source.SplitListSerializer;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollSettings;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollingSourceBuilder;
import com.datasqrl.flinkrunner.connector.camel.source.polling.RateLimiter;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;

/**
 * A Flink source that reads a {@link FileSource} in file mode: it lists the location on every poll,
 * reads each file version it hasn't seen, and keeps the seen-set in Flink state. The location is
 * never modified.
 *
 * <pre>{@code
 * CamelFileSource<Posting> source = CamelFileSource.builder(new GlPostings())
 *         .readiness(CamelFileSource.Readiness.trigger(".done"))
 *         .pollInterval(Duration.ofMinutes(15))
 *         .build();
 *
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "gl-postings");
 * }</pre>
 *
 * <p>A file's version is its path, mtime and size; for a URL it is a hash of the content, fetched
 * with a conditional GET. Every record of a snapshot unit carries the unit's timestamp, and with
 * {@link Builder#pollWatermarks(boolean)} a watermark at that timestamp follows the unit's last
 * record, meaning the unit is complete. A restore re-reads a half-emitted unit from the start with
 * the same timestamp, so downstream deduplicates by key, or by file and row for keyless data.
 *
 * <p>The source has one split, so it reads on a single subtask regardless of parallelism.
 */
@Internal
public final class CamelFileSource<R>
    implements Source<R, FileSplit, List<FileSplit>>, ResultTypeQueryable<R> {

  /** The order files of a listing are read in ({@code file.order}). */
  public enum Order {
    /** By path relative to the location. */
    NAME,
    /** By modification time, then path. */
    MTIME,
    /** In whatever order the listing returns. */
    NONE
  }

  /** What one snapshot is ({@code file.snapshot-unit}). */
  public enum SnapshotUnit {
    /** Each new file stands alone, with its own timestamp. */
    FILE,
    /**
     * All matching files together, for multi-part snapshots: any change to the listing re-reads
     * every file of it at one timestamp.
     */
    LISTING
  }

  /** When a listed file is ready to read ({@code file.ready}). */
  public static final class Readiness implements Serializable {

    enum Kind {
      STABLE_SIZE,
      TRIGGER,
      NONE
    }

    private final Kind kind;
    private final String suffix;

    private Readiness(Kind kind, String suffix) {
      this.kind = kind;
      this.suffix = suffix;
    }

    /**
     * The file's size and mtime stay the same across two checks (Camel's {@code readLock=changed}).
     */
    public static Readiness stableSize() {
      return new Readiness(Kind.STABLE_SIZE, null);
    }

    /** A trigger file named like the file plus {@code suffix}, e.g. {@code .done}, exists. */
    public static Readiness trigger(String suffix) {
      if (suffix == null || suffix.isEmpty()) {
        throw new IllegalArgumentException("A trigger file needs a suffix, e.g. .done");
      }
      return new Readiness(Kind.TRIGGER, suffix);
    }

    /** Every listed file is ready. */
    public static Readiness none() {
      return new Readiness(Kind.NONE, null);
    }

    Kind kind() {
      return kind;
    }

    String suffix() {
      return suffix;
    }

    @Override
    public String toString() {
      return kind == Kind.TRIGGER ? "trigger:" + suffix : kind.name().toLowerCase();
    }
  }

  private final FileSettings<?, ?, R> settings;
  private final TypeInformation<R> producedType;

  private CamelFileSource(FileSettings<?, ?, R> settings, TypeInformation<R> producedType) {
    this.settings = settings;
    this.producedType = producedType;
  }

  /** Emits what the source maps, typed by {@link FileSource}'s first type argument. */
  public static <OUT, IN> Builder<OUT, IN, OUT> builder(FileSource<OUT, IN> source) {
    return new Builder<>(
        source, RecordConverter.identity(), ContractTypes.argument(FileSource.class, source, 0));
  }

  /** Emits what {@code converter} makes of each mapped record. */
  public static <OUT, IN, R> Builder<OUT, IN, R> builder(
      FileSource<OUT, IN> source,
      RecordConverter<OUT, R> converter,
      TypeInformation<R> producedType) {
    return new Builder<>(source, converter, producedType);
  }

  @Override
  public Boundedness getBoundedness() {
    return settings.poll().bounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<R, FileSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return createReader(settings, readerContext);
  }

  private static <OUT, IN, R> SourceReader<R, FileSplit> createReader(
      FileSettings<OUT, IN, R> settings, SourceReaderContext readerContext) throws Exception {
    settings.converter().open(readerContext);
    ClassLoader classLoader = readerContext.getUserCodeClassLoader().asClassLoader();
    PollErrors pollErrors = settings.poll().newPollErrors(settings.location());
    RateLimiter rateLimiter = new RateLimiter(settings.poll().rateLimitPerMinute());
    long timeoutMs = settings.poll().fetchTimeout().toMillis();
    CamelRuntime runtime;
    FileLister lister;
    if (settings.isUrl()) {
      runtime =
          new CamelRuntime(
              classLoader,
              settings.source(),
              settings.componentOptions(),
              settings.location(),
              pollErrors,
              UrlLister::configure);
      lister = new UrlLister(runtime, settings.location(), rateLimiter, timeoutMs);
    } else {
      SeenSetRepository repository = new SeenSetRepository();
      runtime =
          new CamelRuntime(
              classLoader,
              settings.source(),
              settings.componentOptions(),
              settings.location(),
              pollErrors,
              endpoint -> CamelFileLister.configure(endpoint, repository, settings));
      lister = new CamelFileLister(runtime, repository, rateLimiter, timeoutMs);
    }
    Counter dropped = readerContext.metricGroup().counter("numRecordsDropped");
    Counter failed = readerContext.metricGroup().counter("numRecordsFailed");
    return new FileSourceReader<>(
        () -> new FileSplitReader<>(settings, lister, dropped, failed),
        new FileRecordEmitter<>(
            settings.snapshotUnit(),
            settings.seenRetention().toMillis(),
            settings.poll().pollWatermarks()),
        runtime,
        readerContext);
  }

  @Override
  public SplitEnumerator<FileSplit, List<FileSplit>> createEnumerator(
      SplitEnumeratorContext<FileSplit> enumContext) {
    return new CamelSplitEnumerator<>(enumContext, List.of(FileSplit.initial()));
  }

  @Override
  public SplitEnumerator<FileSplit, List<FileSplit>> restoreEnumerator(
      SplitEnumeratorContext<FileSplit> enumContext, List<FileSplit> unassigned) {
    return new CamelSplitEnumerator<>(enumContext, unassigned);
  }

  @Override
  public SimpleVersionedSerializer<FileSplit> getSplitSerializer() {
    return FileSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<List<FileSplit>> getEnumeratorCheckpointSerializer() {
    return new SplitListSerializer<>(FileSplitSerializer.INSTANCE);
  }

  @Override
  public TypeInformation<R> getProducedType() {
    return producedType;
  }

  /** Builds a {@link CamelFileSource}. */
  @Internal
  public static final class Builder<OUT, IN, R> extends PollingSourceBuilder<Builder<OUT, IN, R>> {

    private final FileSource<OUT, IN> source;
    private final RecordConverter<OUT, R> converter;
    private final TypeInformation<R> producedType;
    private Readiness readiness = Readiness.stableSize();
    private Order order = Order.NAME;
    private SnapshotUnit snapshotUnit = SnapshotUnit.FILE;
    private Duration seenRetention = Duration.ofDays(30);
    private double minRowRatio;

    private Builder(
        FileSource<OUT, IN> source,
        RecordConverter<OUT, R> converter,
        TypeInformation<R> producedType) {
      this.source = Objects.requireNonNull(source, "source");
      this.converter = Objects.requireNonNull(converter, "converter");
      this.producedType = Objects.requireNonNull(producedType, "producedType");
    }

    /** When a listed file is ready to read. Defaults to a stable size. Ignored for URLs. */
    public Builder<OUT, IN, R> readiness(Readiness readiness) {
      this.readiness = Objects.requireNonNull(readiness, "readiness");
      return this;
    }

    /** The order files of a listing are read in. Defaults to by name. Ignored for URLs. */
    public Builder<OUT, IN, R> order(Order order) {
      this.order = Objects.requireNonNull(order, "order");
      return this;
    }

    /** What one snapshot is. Defaults to each file. A URL is always one file. */
    public Builder<OUT, IN, R> snapshotUnit(SnapshotUnit snapshotUnit) {
      this.snapshotUnit = Objects.requireNonNull(snapshotUnit, "snapshotUnit");
      return this;
    }

    /**
     * Forgets a version once no listing has contained it for this long. Defaults to 30 days. Only
     * applies to snapshot unit file.
     */
    public Builder<OUT, IN, R> seenRetention(Duration seenRetention) {
      this.seenRetention = Objects.requireNonNull(seenRetention, "seenRetention");
      return this;
    }

    /**
     * Fails the job when a unit has fewer rows than this ratio of the previous unit's, so a
     * truncated download can't look like mass deletion. Off by default.
     */
    public Builder<OUT, IN, R> minRowRatio(double minRowRatio) {
      this.minRowRatio = minRowRatio;
      return this;
    }

    public CamelFileSource<R> build() {
      PollSettings poll = pollSettings();
      String location = Objects.requireNonNull(source.location(), "location()");
      if (minRowRatio < 0 || minRowRatio > 1) {
        throw new IllegalArgumentException(
            "minRowRatio must be between 0 and 1, got " + minRowRatio);
      }
      if (seenRetention.isNegative() || seenRetention.isZero()) {
        throw new IllegalArgumentException("seenRetention must be positive");
      }
      return new CamelFileSource<>(
          new FileSettings<>(
              source,
              converter,
              componentOptions(),
              location,
              poll,
              readiness,
              order,
              FileSettings.isUrl(location) ? SnapshotUnit.FILE : snapshotUnit,
              seenRetention,
              minRowRatio),
          producedType);
    }
  }
}
