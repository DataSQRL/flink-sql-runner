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

import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.connector.camel.source.RecordConverter;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollSettings;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

/** Everything a file-mode source needs at runtime. Shipped to the task managers. */
record FileSettings<OUT, IN, R>(
    FileSource<OUT, IN> source,
    RecordConverter<OUT, R> converter,
    Map<String, String> componentOptions,
    String location,
    PollSettings poll,
    CamelFileSource.Readiness readiness,
    CamelFileSource.Order order,
    CamelFileSource.SnapshotUnit snapshotUnit,
    Duration seenRetention,
    double minRowRatio)
    implements Serializable {

  /** Whether the location is a URL, read with a conditional GET, rather than a listing. */
  boolean isUrl() {
    return isUrl(location);
  }

  static boolean isUrl(String location) {
    return location.startsWith("http:") || location.startsWith("https:");
  }
}
