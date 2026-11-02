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
package com.datasqrl.flinkrunner.stdlib.typesafe;

import java.util.Arrays;
import java.util.Optional;

/** What happens to a row when classification fails with a non-fatal error. */
public enum ErrorPolicy {
  /** Throw, failing the job so that it restarts per its restart strategy. */
  FAIL,
  /** Emit the row with all answers NULL and the error in the {@code _error} column. */
  NULL;

  static Optional<ErrorPolicy> parse(String value) {
    return Arrays.stream(values()).filter(p -> p.name().equalsIgnoreCase(value)).findFirst();
  }
}
