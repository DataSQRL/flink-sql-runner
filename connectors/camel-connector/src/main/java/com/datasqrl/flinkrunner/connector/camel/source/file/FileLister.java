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

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.camel.Exchange;

/**
 * Lists a file-mode location one file at a time, on the fetcher thread. A listing starts with
 * {@link #startListing()} and ends when {@link #next} returns {@link #DONE}.
 */
interface FileLister {

  /** Returned by {@link #next} when the listing has no further files to read. */
  ListedFile DONE = new ListedFile(null, null, null);

  /** A file version to read, and the exchange holding its content. */
  record ListedFile(String key, String name, Exchange exchange) {}

  void startListing();

  /**
   * The next file of the listing whose version {@code skip} doesn't reject, {@link #DONE}, or
   * {@code null} when nothing could be fetched right now (an error being retried, or a backoff), so
   * the caller tries again later.
   */
  ListedFile next(Predicate<String> skip) throws IOException;

  /** Every version the current listing contained so far, skipped or not. */
  Set<String> listed();

  /** Done with a file returned by {@link #next}. */
  void release(ListedFile file);
}
