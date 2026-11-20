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
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.CamelSplitEnumerator;
import com.datasqrl.flinkrunner.connector.camel.source.ContractTypes;
import com.datasqrl.flinkrunner.connector.camel.source.PollErrors;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.Counter;

/**
 * A Flink source that reads an {@link AckSource} from its Camel consumer endpoint, in ack mode:
 * messages are acknowledged to the source system only after a Flink checkpoint containing them
 * completes.
 *
 * <pre>{@code
 * CamelAckSource<Order> source = CamelAckSource.builder(new Orders()).build();
 *
 * env.fromSource(source, WatermarkStrategy.noWatermarks(), "camel");
 * }</pre>
 *
 * <p>The source has one split, so it reads on a single subtask regardless of parallelism. Delivery
 * is at-least-once. Every record carries the timestamp of the poll that received it.
 *
 * <p>At-least-once needs a component that commits when its exchange completes. One that commits
 * inside its own {@code receive} loses what no checkpoint covered when the job fails. camel-jms and
 * the components built on it do, and are read through a transacted session instead; no other
 * component is checked.
 */
@Internal
public final class CamelAckSource<R>
    implements Source<R, AckSplit, List<AckSplit>>, ResultTypeQueryable<R> {

  /**
   * Exchanges Camel's polling consumer may hold that were taken from the source system but not yet
   * received. Zero would make its queue unbounded.
   */
  private static final int FETCH_AHEAD = 1;

  private final AckSettings<?, R> settings;
  private final TypeInformation<R> producedType;

  private CamelAckSource(AckSettings<?, R> settings, TypeInformation<R> producedType) {
    this.settings = settings;
    this.producedType = producedType;
  }

  /** Emits what the source maps, typed by {@link AckSource}'s type argument. */
  public static <OUT> Builder<OUT, OUT> builder(AckSource<OUT> source) {
    return new Builder<>(
        source, RecordConverter.identity(), ContractTypes.argument(AckSource.class, source, 0));
  }

  /** Emits what {@code converter} makes of each mapped record. */
  public static <OUT, R> Builder<OUT, R> builder(
      AckSource<OUT> source, RecordConverter<OUT, R> converter, TypeInformation<R> producedType) {
    return new Builder<>(source, converter, producedType);
  }

  @Override
  public Boundedness getBoundedness() {
    return settings.isBounded() ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SourceReader<R, AckSplit> createReader(SourceReaderContext readerContext)
      throws Exception {
    return createReader(settings, readerContext);
  }

  private static <OUT, R> SourceReader<R, AckSplit> createReader(
      AckSettings<OUT, R> settings, SourceReaderContext readerContext) throws Exception {
    settings.converter().open(readerContext);
    PollErrors pollErrors =
        settings.retriesFetchErrors()
            ? new PollErrors(
                settings.endpointUri(),
                settings.retryMaxAttempts(),
                settings.retryInitialBackoff(),
                settings.retryMaxBackoff())
            : null;
    CamelRuntime runtime =
        new CamelRuntime(
            readerContext.getUserCodeClassLoader().asClassLoader(),
            settings.source(),
            settings.componentOptions(),
            settings.endpointUri(),
            pollErrors,
            CamelAckSource::limitFetchAhead);
    PendingAcks pendingAcks = new PendingAcks();
    Counter dropped = readerContext.metricGroup().counter("numRecordsDropped");
    Counter failed = readerContext.metricGroup().counter("numRecordsFailed");
    return new AckSourceReader<>(
        () -> new AckSplitReader<>(settings, runtime, dropped, failed),
        runtime,
        pendingAcks,
        new Configuration(),
        readerContext);
  }

  @Override
  public SplitEnumerator<AckSplit, List<AckSplit>> createEnumerator(
      SplitEnumeratorContext<AckSplit> enumContext) {
    return new CamelSplitEnumerator<>(enumContext, List.of(new AckSplit()));
  }

  @Override
  public SplitEnumerator<AckSplit, List<AckSplit>> restoreEnumerator(
      SplitEnumeratorContext<AckSplit> enumContext, List<AckSplit> unassigned) {
    return new CamelSplitEnumerator<>(enumContext, unassigned);
  }

  @Override
  public SimpleVersionedSerializer<AckSplit> getSplitSerializer() {
    return AckSplitSerializer.INSTANCE;
  }

  @Override
  public SimpleVersionedSerializer<List<AckSplit>> getEnumeratorCheckpointSerializer() {
    return AckEnumeratorStateSerializer.INSTANCE;
  }

  @Override
  public TypeInformation<R> getProducedType() {
    return producedType;
  }

  /** Builds a {@link CamelAckSource}. */
  @Internal
  public static final class Builder<OUT, R> {

    private final AckSource<OUT> source;
    private final RecordConverter<OUT, R> converter;
    private final TypeInformation<R> producedType;
    private Map<String, String> componentOptions = Map.of();
    private Duration receiveTimeout = Duration.ofSeconds(1);
    private int maxPending = 10_000;
    private int stopAfterEmptyPolls;
    private boolean pollWatermarks;
    private int retryMaxAttempts;
    private Duration retryInitialBackoff = Duration.ofMinutes(1);
    private Duration retryMaxBackoff = Duration.ofMinutes(30);

    private Builder(
        AckSource<OUT> source, RecordConverter<OUT, R> converter, TypeInformation<R> producedType) {
      this.source = Objects.requireNonNull(source, "source");
      this.converter = Objects.requireNonNull(converter, "converter");
      this.producedType = Objects.requireNonNull(producedType, "producedType");
    }

    /**
     * Camel component options, keyed {@code <component>.<option>} such as {@code
     * salesforce.clientId}. The component name ends at the first dot. An unknown option fails the
     * reader.
     */
    public Builder<OUT, R> componentOptions(Map<String, String> componentOptions) {
      this.componentOptions = Map.copyOf(componentOptions);
      return this;
    }

    /** How long one receive waits for an exchange. Defaults to 1s. */
    public Builder<OUT, R> receiveTimeout(Duration receiveTimeout) {
      this.receiveTimeout = receiveTimeout;
      return this;
    }

    /** Max exchanges held while waiting for a checkpoint. Defaults to 10,000. */
    public Builder<OUT, R> maxPending(int maxPending) {
      this.maxPending = maxPending;
      return this;
    }

    /**
     * Makes the source bounded: it finishes after this many receives in a row return nothing. By
     * default the source is unbounded.
     */
    public Builder<OUT, R> stopAfterEmptyPolls(int stopAfterEmptyPolls) {
      this.stopAfterEmptyPolls = stopAfterEmptyPolls;
      return this;
    }

    /**
     * Emits a watermark at the poll timestamp after every poll that received exchanges, then marks
     * the source idle until the next one. Off by default.
     */
    public Builder<OUT, R> pollWatermarks(boolean pollWatermarks) {
      this.pollWatermarks = pollWatermarks;
      return this;
    }

    /**
     * Retries failed fetches with exponential backoff from {@code initialBackoff} up to {@code
     * maxBackoff}, and fails the job after {@code maxAttempts} failures in a row. Off by default:
     * Camel then logs poll errors and keeps polling, and the source only sees empty receives.
     */
    public Builder<OUT, R> retry(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
      this.retryMaxAttempts = maxAttempts;
      this.retryInitialBackoff = initialBackoff;
      this.retryMaxBackoff = maxBackoff;
      return this;
    }

    public CamelAckSource<R> build() {
      String endpointUri = Objects.requireNonNull(source.endpoint(), "endpoint()");
      if (maxPending <= 0) {
        throw new IllegalArgumentException("maxPending must be positive");
      }
      return new CamelAckSource<>(
          new AckSettings<>(
              source,
              converter,
              componentOptions,
              endpointUri,
              receiveTimeout,
              maxPending,
              stopAfterEmptyPolls,
              pollWatermarks,
              retryMaxAttempts,
              retryInitialBackoff,
              retryMaxBackoff),
          producedType);
    }
  }

  /**
   * Camel's polling consumer queues what the endpoint's consumer fetched ahead of {@code receive},
   * up to 1000 exchanges by default. The source system counts those as delivered, but {@code
   * maxPending} only sees received exchanges, so the queue is cut to {@link #FETCH_AHEAD}, and its
   * consumer waits for room instead of failing exchanges. A batch the component's own poll already
   * fetched, such as SQS's {@code maxMessagesPerPoll}, still waits in the consumer's hands.
   */
  private static void limitFetchAhead(Endpoint endpoint) {
    if (endpoint instanceof DefaultEndpoint fetching) {
      fetching.setPollingConsumerQueueSize(FETCH_AHEAD);
      fetching.setPollingConsumerBlockWhenFull(true);
    }
  }
}
