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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.service.ServiceSupport;

/**
 * Plugs the file-mode seen-set into Camel's file consumers as their idempotent repository.
 *
 * <p>With {@code idempotentEager=false} the consumer calls {@link #contains} for every matching
 * file while listing, after its include, exclude and done-file checks. That call tells the fetcher
 * which versions the listing holds, and answers whether to skip the file. Camel's own bookkeeping
 * ({@code add} on commit, {@code remove} on rollback) is ignored: the seen-set changes only in
 * Flink state, when the emitter has emitted a whole file. Because the fetcher rejects a version as
 * soon as it has received it, a file can't be delivered again while it is being read, which
 * ack-mode SFTP does.
 *
 * <p>Called on the fetcher thread, which runs the polling consumer's polls.
 */
final class SeenSetRepository extends ServiceSupport implements IdempotentRepository {

  private final Set<String> listed = new HashSet<>();
  private Predicate<String> skip = key -> false;

  synchronized void startListing() {
    listed.clear();
  }

  synchronized void skip(Predicate<String> skip) {
    this.skip = skip;
  }

  synchronized Set<String> listed() {
    return Set.copyOf(listed);
  }

  @Override
  public synchronized boolean contains(String key) {
    listed.add(key);
    return skip.test(key);
  }

  @Override
  public boolean add(String key) {
    return true;
  }

  @Override
  public boolean remove(String key) {
    return true;
  }

  @Override
  public boolean confirm(String key) {
    return true;
  }

  @Override
  public void clear() {}
}
