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
package com.datasqrl.flinkrunner.connector.camel.table;

import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.ACK_EMPTY_POLLS;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.ACK_MAX_PENDING;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.CURSOR_LOOKBACK;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.CURSOR_RESET;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.FETCH_TIMEOUT;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.FILE_MIN_ROW_RATIO;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.FILE_ORDER;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.FILE_READY;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.FILE_SEEN_RETENTION;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.FILE_SNAPSHOT_UNIT;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.POLL_INTERVAL;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.RATE_LIMIT;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.RETRY_INITIAL_BACKOFF;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.RETRY_MAX_ATTEMPTS;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.RETRY_MAX_BACKOFF;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.connector.camel.source.ack.CamelAckSource;
import com.datasqrl.flinkrunner.connector.camel.source.cursor.CamelCursorSource;
import com.datasqrl.flinkrunner.connector.camel.source.file.CamelFileSource;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollingSourceBuilder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * An append-only table over a user's Camel source, read in the mode its contract gives. The
 * physical columns are the source's record components; metadata columns ({@link ReadableMetadata})
 * follow them. With {@code WATERMARK FOR ingested_at AS SOURCE_WATERMARK()} the source emits a
 * watermark at each unit's timestamp once the unit's records are out.
 */
final class CamelDynamicTableSource
    implements ScanTableSource, SupportsReadingMetadata, SupportsSourceWatermark {

  private static final String READY_STABLE_SIZE = "stable-size";
  private static final String READY_NONE = "none";
  private static final String READY_TRIGGER_PREFIX = "trigger:";

  private final UserSource source;
  private final DataType recordType;
  private final ReadableConfig options;
  private final Map<String, String> componentOptions;
  private final boolean bounded;

  private List<String> metadataKeys = List.of();
  private DataType producedDataType;
  private boolean sourceWatermark;

  CamelDynamicTableSource(
      UserSource source,
      DataType recordType,
      DataType physicalDataType,
      ReadableConfig options,
      Map<String, String> componentOptions,
      boolean bounded) {
    this.source = source;
    this.recordType = recordType;
    this.options = options;
    this.componentOptions = componentOptions;
    this.bounded = bounded;
    this.producedDataType = physicalDataType;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
    RowConverter converter =
        new RowConverter(
            context.createDataStructureConverter(recordType),
            metadataKeys.stream().map(ReadableMetadata::of).toList());
    TypeInformation<RowData> producedType = context.createTypeInformation(producedDataType);
    // The source has a single split, so any other subtask would only sit idle.
    return SourceProvider.of(build(converter, producedType), 1);
  }

  @SuppressWarnings("unchecked")
  private Source<RowData, ?, ?> build(RowConverter converter, TypeInformation<RowData> type) {
    Object instance = source.instance();
    return switch (source.mode()) {
      case ACK -> {
        CamelAckSource.Builder<Object, RowData> builder =
            CamelAckSource.builder((AckSource<Object>) instance, converter, type)
                .componentOptions(componentOptions)
                .receiveTimeout(options.get(FETCH_TIMEOUT))
                .maxPending(options.get(ACK_MAX_PENDING))
                .pollWatermarks(sourceWatermark)
                .retry(
                    options.get(RETRY_MAX_ATTEMPTS),
                    options.get(RETRY_INITIAL_BACKOFF),
                    options.get(RETRY_MAX_BACKOFF));
        if (bounded) {
          builder.stopAfterEmptyPolls(options.get(ACK_EMPTY_POLLS));
        }
        yield builder.build();
      }
      case CURSOR ->
          polling(
                  CamelCursorSource.builder(
                      (CursorSource<Object, Object, Object>) instance, converter, type))
              .fullRefresh(options.get(CURSOR_RESET) == CamelConnectorOptions.CursorReset.PER_POLL)
              .lookback(options.get(CURSOR_LOOKBACK))
              .build();
      case FILE -> {
        CamelFileSource.Builder<Object, Object, RowData> builder =
            polling(CamelFileSource.builder((FileSource<Object, Object>) instance, converter, type))
                .readiness(readiness(options.get(FILE_READY)))
                .order(options.get(FILE_ORDER))
                .snapshotUnit(options.get(FILE_SNAPSHOT_UNIT))
                .seenRetention(options.get(FILE_SEEN_RETENTION));
        options.getOptional(FILE_MIN_ROW_RATIO).ifPresent(builder::minRowRatio);
        yield builder.build();
      }
    };
  }

  private <B extends PollingSourceBuilder<B>> B polling(B builder) {
    builder
        .componentOptions(componentOptions)
        .fetchTimeout(options.get(FETCH_TIMEOUT))
        .pollInterval(options.get(POLL_INTERVAL))
        .bounded(bounded)
        .pollWatermarks(sourceWatermark)
        .retry(
            options.get(RETRY_MAX_ATTEMPTS),
            options.get(RETRY_INITIAL_BACKOFF),
            options.get(RETRY_MAX_BACKOFF));
    options.getOptional(RATE_LIMIT).ifPresent(builder::rateLimit);
    return builder;
  }

  private static CamelFileSource.Readiness readiness(String value) {
    if (value.equals(READY_STABLE_SIZE)) {
      return CamelFileSource.Readiness.stableSize();
    }
    if (value.equals(READY_NONE)) {
      return CamelFileSource.Readiness.none();
    }
    if (value.startsWith(READY_TRIGGER_PREFIX)) {
      return CamelFileSource.Readiness.trigger(value.substring(READY_TRIGGER_PREFIX.length()));
    }
    throw new ValidationException(
        "'"
            + FILE_READY.key()
            + "' must be '"
            + READY_STABLE_SIZE
            + "', '"
            + READY_NONE
            + "' or '"
            + READY_TRIGGER_PREFIX
            + "<suffix>', got '"
            + value
            + "'");
  }

  @Override
  public Map<String, DataType> listReadableMetadata() {
    Map<String, DataType> metadata = new LinkedHashMap<>();
    for (ReadableMetadata m : ReadableMetadata.values()) {
      if (m.appliesTo(source.mode())) {
        metadata.put(m.key, m.dataType);
      }
    }
    return metadata;
  }

  @Override
  public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
    this.metadataKeys = List.copyOf(metadataKeys);
    this.producedDataType = producedDataType;
  }

  @Override
  public void applySourceWatermark() {
    this.sourceWatermark = true;
  }

  @Override
  public DynamicTableSource copy() {
    CamelDynamicTableSource copy =
        new CamelDynamicTableSource(
            source, recordType, producedDataType, options, componentOptions, bounded);
    copy.metadataKeys = metadataKeys;
    copy.sourceWatermark = sourceWatermark;
    return copy;
  }

  @Override
  public String asSummaryString() {
    return "Camel";
  }
}
