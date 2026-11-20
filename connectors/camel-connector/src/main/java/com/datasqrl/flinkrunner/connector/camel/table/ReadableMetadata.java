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

import static com.datasqrl.flinkrunner.connector.camel.table.SourceMode.ACK;
import static com.datasqrl.flinkrunner.connector.camel.table.SourceMode.FILE;

import com.datasqrl.flinkrunner.connector.camel.source.RecordContext;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.camel.Exchange;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

/** Metadata columns a Camel table can declare with {@code METADATA [FROM key]}. */
enum ReadableMetadata {
  /** The timestamp of the unit the record belongs to, shared by every record of the unit. */
  INGESTED_AT(
      "ingested_at",
      DataTypes.TIMESTAMP_LTZ(3).notNull(),
      EnumSet.allOf(SourceMode.class),
      context -> TimestampData.fromEpochMillis(context.ingestedAt())),
  /** The file's path relative to the location, or the URL. */
  FILE_NAME(
      "file",
      DataTypes.STRING().notNull(),
      EnumSet.of(FILE),
      context ->
          StringData.fromString(
              context.exchange().getMessage().getHeader(Exchange.FILE_NAME, String.class))),
  /** The 1-based ordinal of the record within its file, counting dropped and failed records. */
  ROW("row", DataTypes.BIGINT().notNull(), EnumSet.of(FILE), RecordContext::row),
  /** Camel's id for the exchange. */
  EXCHANGE_ID(
      "exchange_id",
      DataTypes.STRING().notNull(),
      EnumSet.of(ACK),
      context -> StringData.fromString(context.exchange().getExchangeId())),
  /**
   * The message headers, with values converted to strings by Camel's type converters. A value that
   * does not convert is null.
   */
  HEADERS(
      "headers",
      DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.STRING()).notNull(),
      EnumSet.of(ACK),
      context -> headers(context.exchange()));

  final String key;
  final DataType dataType;
  private final Set<SourceMode> modes;
  private final Function<RecordContext, Object> reader;

  ReadableMetadata(
      String key,
      DataType dataType,
      Set<SourceMode> modes,
      Function<RecordContext, Object> reader) {
    this.key = key;
    this.dataType = dataType;
    this.modes = modes;
    this.reader = reader;
  }

  boolean appliesTo(SourceMode mode) {
    return modes.contains(mode);
  }

  Object read(RecordContext context) {
    return reader.apply(context);
  }

  static ReadableMetadata of(String key) {
    for (ReadableMetadata metadata : values()) {
      if (metadata.key.equals(key)) {
        return metadata;
      }
    }
    throw new IllegalArgumentException("Unknown Camel metadata key " + key);
  }

  private static Object headers(Exchange exchange) {
    Map<StringData, StringData> headers = new HashMap<>();
    exchange
        .getMessage()
        .getHeaders()
        .forEach(
            (name, value) -> {
              String text =
                  value == null
                      ? null
                      : exchange
                          .getContext()
                          .getTypeConverter()
                          .tryConvertTo(String.class, exchange, value);
              headers.put(StringData.fromString(name), StringData.fromString(text));
            });
    return new GenericMapData(headers);
  }
}
