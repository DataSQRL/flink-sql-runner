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

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Records read lazily from an open stream, which the file reader closes once the file is read.
 * Iterable once only.
 */
final class StreamedRecords<T> implements Iterable<T>, AutoCloseable {

  private final Iterator<T> records;
  private final Closeable stream;

  StreamedRecords(Iterator<T> records, Closeable stream) {
    this.records = records;
    this.stream = stream;
  }

  @Override
  public Iterator<T> iterator() {
    return records;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
