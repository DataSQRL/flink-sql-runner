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
package com.datasqrl.flinkrunner.camel;

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.camel.Exchange;

/**
 * A source system that can return everything since a position, read in cursor mode by
 * CamelCursorSource.
 *
 * <p>Each poll sends {@link #request(Object)} for the current position, splits the response into
 * records, maps them and follows {@link #next(Exchange)} page by page. The position advances record
 * by record. When the last page moved the position, the source requests again straight away;
 * otherwise the poll is complete. Records must come in position order. Overlapping cursors ({@code
 * >=}) must page with {@code next}, never with a LIMIT, so a burst of rows sharing one position
 * can't be cut off.
 *
 * <p>With a lookback, each request starts at {@link #rewind(Object, Duration)} of the position, so
 * rows that concurrent writers committed late are read on the next request. Re-read records are
 * emitted again but never move the position back.
 *
 * <p>All methods run on the fetcher thread, never on the task's mailbox thread.
 *
 * @param <OUT> the emitted record
 * @param <POS> the position, e.g. an {@code Instant} or a record for a composite cursor
 * @param <IN> one raw record of a response, as {@link #records(Exchange)} returns it
 */
public interface CursorSource<OUT, POS, IN> extends Serializable {

  /** The starting position. */
  POS initial();

  /** The request for everything since {@code position}. */
  Request request(POS position);

  /**
   * The request for the next page of the same result, such as a page token or next-records URL. The
   * default is no further pages.
   */
  default Optional<Request> next(Exchange response) {
    return Optional.empty();
  }

  /**
   * Splits a response into raw records. The default handles bodies that are an {@link Iterable} or
   * an array; any other non-null body is one record.
   */
  @SuppressWarnings("unchecked")
  default Iterable<IN> records(Exchange response) {
    Object body = response.getMessage().getBody();
    if (body == null) {
      return List.of();
    }
    if (body instanceof Iterable<?> iterable) {
      return (Iterable<IN>) iterable;
    }
    if (body instanceof Object[] array) {
      return (List<IN>) Arrays.asList(array);
    }
    return List.of((IN) body);
  }

  /** Per-record filter/map. Returning {@code null} drops the record; throwing skips it. */
  OUT map(IN record) throws Exception;

  /** The position of an emitted record. */
  POS position(OUT record);

  /**
   * The position {@code window} before {@code position}, where a request with a lookback starts.
   * The default handles positions that are a {@link Temporal} with a time of day, such as an {@code
   * Instant}; a composite cursor overrides it.
   */
  @SuppressWarnings("unchecked")
  default POS rewind(POS position, Duration window) {
    if (position instanceof Temporal time) {
      return (POS) time.minus(window);
    }
    throw new UnsupportedOperationException(
        "Cannot rewind the position " + position + ", override CursorSource.rewind");
  }
}
