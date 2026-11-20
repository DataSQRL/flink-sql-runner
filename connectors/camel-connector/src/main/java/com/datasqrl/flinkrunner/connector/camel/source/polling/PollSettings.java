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
package com.datasqrl.flinkrunner.connector.camel.source.polling;

import com.datasqrl.flinkrunner.connector.camel.source.PollErrors;
import java.io.Serializable;
import java.time.Duration;
import org.apache.flink.annotation.Internal;

/** The polling options that cursor and file mode share. Shipped to the task managers. */
@Internal
public record PollSettings(
    Duration pollInterval,
    Duration fetchTimeout,
    boolean bounded,
    boolean pollWatermarks,
    int rateLimitPerMinute,
    int retryMaxAttempts,
    Duration retryInitialBackoff,
    Duration retryMaxBackoff)
    implements Serializable {

  /** Tracks fetch errors when retries are on, otherwise {@code null}: errors fail the fetch. */
  public PollErrors newPollErrors(String endpointUri) {
    return retryMaxAttempts > 0
        ? new PollErrors(endpointUri, retryMaxAttempts, retryInitialBackoff, retryMaxBackoff)
        : null;
  }
}
