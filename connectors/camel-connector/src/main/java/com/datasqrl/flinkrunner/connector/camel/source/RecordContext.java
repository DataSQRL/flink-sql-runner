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

import org.apache.camel.Exchange;
import org.apache.flink.annotation.Internal;

/**
 * Where a mapped record came from, for a {@link RecordConverter}.
 *
 * @param ingestedAt the unit's timestamp in epoch milliseconds, the same value the record carries
 *     as its Flink timestamp
 * @param exchange the received message in ack mode, the response page in cursor mode, the file in
 *     file mode
 * @param row the 1-based ordinal of the raw record within {@code exchange}, counting dropped and
 *     failed records
 */
@Internal
public record RecordContext(long ingestedAt, Exchange exchange, long row) {}
