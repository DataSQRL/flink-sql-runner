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
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import org.apache.camel.Exchange;

/**
 * A received exchange and its mapped value, handed from the fetcher thread to the emitter. The
 * value is {@code null} when the mapper dropped the record or failed on it; the exchange is still
 * acknowledged. The timestamp is the poll timestamp shared by every record of one fetch.
 *
 * <p>A record without an exchange marks the end of a poll, after which the emitter may emit a
 * watermark at the poll timestamp.
 */
record AckRecord<OUT>(OUT value, Exchange exchange, long timestamp) {

  static <OUT> AckRecord<OUT> endOfPoll(long timestamp) {
    return new AckRecord<>(null, null, timestamp);
  }

  boolean isEndOfPoll() {
    return exchange == null;
  }
}
