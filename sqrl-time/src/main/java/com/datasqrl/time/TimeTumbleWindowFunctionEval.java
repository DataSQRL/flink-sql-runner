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

import java.time.Instant;

public interface TimeTumbleWindowFunctionEval {

  Instant eval(Instant instant, Long multiple, Long offset);

  default Instant eval(Instant instant, Long multiple) {
    return eval(instant, multiple, 0L);
  }

  default Instant eval(Instant instant) {
    return eval(instant, 1L);
  }
}
