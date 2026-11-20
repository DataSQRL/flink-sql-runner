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
package com.datasqrl.flinkrunner.connector.camel.source;

import java.io.Serializable;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReaderContext;

/**
 * Turns a mapped record into what the source emits. Runs on the fetcher thread after the user's
 * {@code map}. Returning {@code null} drops the record. Throwing is handled like a throwing {@code
 * map}: the record is logged, counted as failed, and still counts as consumed.
 */
@Internal
public interface RecordConverter<OUT, R> extends Serializable {

  /** Called once per source reader, before the first {@link #convert}. */
  default void open(SourceReaderContext context) throws Exception {}

  R convert(OUT value, RecordContext context) throws Exception;

  static <T> RecordConverter<T, T> identity() {
    return (value, context) -> value;
  }
}
