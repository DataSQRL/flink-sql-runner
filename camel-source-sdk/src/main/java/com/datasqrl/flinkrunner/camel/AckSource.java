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
 * A source system that tracks delivery and redelivers unacknowledged messages, read in ack mode.
 *
 * <p>{@link #map(Exchange)} is the per-record filter/map applied to every exchange the endpoint
 * delivers. It runs on the fetcher thread, never on the task's mailbox thread. Returning {@code
 * null} drops the record. Throwing skips the record and logs it. Dropped and failed exchanges are
 * still acknowledged once a checkpoint covering them completes, so they are not redelivered
 * forever.
 *
 * @param <OUT> the emitted record
 */
public interface AckSource<OUT> extends Serializable {

  /** The Camel consumer endpoint URI. */
  String endpoint();

  /** Per-record filter/map. Returning {@code null} drops the record; throwing skips it. */
  OUT map(Exchange exchange) throws Exception;
}
