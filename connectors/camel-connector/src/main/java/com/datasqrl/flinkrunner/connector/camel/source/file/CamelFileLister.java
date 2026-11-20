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

import com.datasqrl.flinkrunner.connector.camel.source.CamelRuntime;
import com.datasqrl.flinkrunner.connector.camel.source.polling.RateLimiter;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.Expression;
import org.apache.camel.component.file.GenericFile;
import org.apache.camel.component.file.GenericFileEndpoint;
import org.apache.camel.support.ExpressionAdapter;
import org.apache.camel.util.URISupport;

/**
 * Lists a {@code file:}, {@code ftp:}, {@code ftps:} or {@code sftp:} location through Camel's
 * polling consumer. Each receive lists the directory and returns the first file, in {@code
 * file.order}, whose version isn't skipped; an empty exchange means none is left. Readiness rules
 * and include/exclude patterns are applied by Camel while listing.
 */
final class CamelFileLister implements FileLister {

  private final CamelRuntime runtime;
  private final SeenSetRepository repository;
  private final RateLimiter rateLimiter;
  private final long timeoutMs;

  CamelFileLister(
      CamelRuntime runtime, SeenSetRepository repository, RateLimiter rateLimiter, long timeoutMs) {
    this.runtime = runtime;
    this.repository = repository;
    this.rateLimiter = rateLimiter;
    this.timeoutMs = timeoutMs;
  }

  @Override
  public void startListing() {
    repository.startListing();
  }

  @Override
  public ListedFile next(Predicate<String> skip) throws IOException {
    repository.skip(skip);
    rateLimiter.acquire();
    Exchange exchange = runtime.receiveWithRetries(timeoutMs);
    if (exchange == null) {
      return null;
    }
    GenericFile<?> file =
        exchange.getProperty(ExchangePropertyKey.FILE_EXCHANGE_FILE, GenericFile.class);
    if (file == null) {
      // sendEmptyMessageWhenIdle: the listing had nothing left to read.
      runtime.done(exchange);
      return DONE;
    }
    return new ListedFile(versionOf(file), file.getRelativeFilePath(), exchange);
  }

  @Override
  public Set<String> listed() {
    return repository.listed();
  }

  @Override
  public void release(ListedFile file) {
    runtime.done(file.exchange());
  }

  /** A file's identity: its path relative to the location, its mtime and its size. */
  static String versionOf(GenericFile<?> file) {
    return file.getRelativeFilePath() + "@" + file.getLastModified() + "@" + file.getFileLength();
  }

  /**
   * Turns the user's endpoint into a read-only lister: never move or delete files, track versions
   * in the seen-set, sort and hold back files that aren't ready yet.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static void configure(
      Endpoint endpoint, SeenSetRepository repository, FileSettings<?, ?, ?> settings) {
    if (!(endpoint instanceof GenericFileEndpoint<?> files)) {
      throw new IllegalArgumentException(
          "File mode lists file:, ftp:, ftps: and sftp: endpoints and reads http(s) URLs, "
              + "but "
              + URISupport.sanitizeUri(endpoint.getEndpointUri())
              + " is none of them. Object stores belong on Flink's FileSource.");
    }
    if (files.isDelete() || files.getMove() != null || files.getPreMove() != null) {
      throw new IllegalArgumentException(
          "File mode never modifies the source location, remove delete, move and preMove from "
              + URISupport.sanitizeUri(endpoint.getEndpointUri()));
    }
    files.setNoop(true);
    files.setIdempotent(true);
    files.setIdempotentEager(false);
    files.setIdempotentRepository(repository);
    files.setIdempotentKey(VERSION);
    // Sort the whole listing before picking a file, not just the first file found.
    files.setEagerMaxMessagesPerPoll(false);
    // An empty exchange instead of a receive timeout marks the end of a listing.
    files.setSendEmptyMessageWhenIdle(true);

    switch (settings.readiness().kind()) {
      case STABLE_SIZE -> files.setReadLock("changed");
      case TRIGGER -> files.setDoneFileName("${file:name}" + settings.readiness().suffix());
      case NONE -> files.setReadLock("none");
    }
    Comparator<GenericFile<?>> byName = Comparator.comparing(GenericFile::getRelativeFilePath);
    switch (settings.order()) {
      case NAME -> files.setSorter((Comparator) byName);
      case MTIME ->
          files.setSorter(
              (Comparator)
                  Comparator.<GenericFile<?>>comparingLong(GenericFile::getLastModified)
                      .thenComparing(byName));
      case NONE -> {}
    }
  }

  private static final Expression VERSION =
      new ExpressionAdapter() {
        @Override
        public Object evaluate(Exchange exchange) {
          GenericFile<?> file =
              exchange.getProperty(ExchangePropertyKey.FILE_EXCHANGE_FILE, GenericFile.class);
          return file == null ? null : versionOf(file);
        }
      };
}
