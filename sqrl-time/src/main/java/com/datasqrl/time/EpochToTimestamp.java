/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.time;

import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

/**
 * Converts the epoch timestamp in seconds to the corresponding timestamp. E.g.
 * epochToTimestamp(1678645414) returns the timestamp 2023-03-12T18:23:34Z
 */
@AutoService(StandardLibraryFunction.class)
public class EpochToTimestamp extends AbstractEpochToTimestamp {

  public EpochToTimestamp() {
    super(false);
  }
}
