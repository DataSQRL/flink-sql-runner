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

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/** Everything an ack-mode source needs at runtime. Shipped to the task managers with the source. */
record AckSettings<OUT, R>(
    AckSource<OUT> source,
    RecordConverter<OUT, R> converter,
    Map<String, String> componentOptions,
    String endpointUri,
    Duration receiveTimeout,
    int maxPending,
    int stopAfterEmptyPolls,
    boolean pollWatermarks,
    int retryMaxAttempts,
    Duration retryInitialBackoff,
    Duration retryMaxBackoff)
    implements Serializable {

  /** A bounded source finishes after this many receives in a row return nothing. */
  boolean isBounded() {
    return stopAfterEmptyPolls > 0;
  }

  /** Fetch errors are retried and fail the job only when this is set. */
  boolean retriesFetchErrors() {
    return retryMaxAttempts > 0;
  }
}
