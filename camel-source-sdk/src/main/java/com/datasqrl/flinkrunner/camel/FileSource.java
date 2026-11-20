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
import org.apache.camel.Exchange;

/**
 * Data that arrives as files or published documents, read in file mode by CamelFileSource.
 *
 * <p>Every poll lists the location and reads each file version not read before: its records are
 * parsed by {@link #records(Exchange)} and mapped one by one. The location is never modified.
 *
 * <p>All methods run on the fetcher thread, never on the task's mailbox thread.
 *
 * @param <OUT> the emitted record
 * @param <IN> one raw record of a file, as {@link #records(Exchange)} returns it
 */
public interface FileSource<OUT, IN> extends Serializable {

  /**
   * The location to list: a {@code file:}, {@code ftp:}, {@code ftps:} or {@code sftp:} consumer
   * endpoint, or one {@code http:} or {@code https:} URL.
   */
  String location();

  /**
   * Parses one file. The exchange body is the file's content, convertible to {@code String}, {@code
   * InputStream} or {@code byte[]}, and the {@code CamelFileName} header its path relative to the
   * location, or the URL. The returned records may be read lazily; if the iterable or its iterator
   * is {@link AutoCloseable}, it is closed once the file is read.
   */
  Iterable<IN> records(Exchange file) throws Exception;

  /** Per-record filter/map. Returning {@code null} drops the record; throwing skips it. */
  OUT map(IN record) throws Exception;
}
