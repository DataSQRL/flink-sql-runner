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
package com.datasqrl.flinkrunner.connector.camel.table;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.FileSource;
import java.util.Locale;

/** How a source reads, given by the contract its class implements. */
enum SourceMode {
  CURSOR(CursorSource.class),
  ACK(AckSource.class),
  FILE(FileSource.class);

  private final Class<?> contract;

  SourceMode(Class<?> contract) {
    this.contract = contract;
  }

  Class<?> contract() {
    return contract;
  }

  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
