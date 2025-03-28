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
import java.time.temporal.ChronoUnit;

/**
 * Time window function that returns the end of minute for the timestamp argument. E.g.
 * endOfMinute(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the timestamp
 * 2023-03-12T18:23:59.999999999Z
 */
@AutoService(StandardLibraryFunction.class)
public class EndOfMinute extends TimeTumbleWindowFunction {

  public EndOfMinute() {
    super(ChronoUnit.MINUTES, ChronoUnit.SECONDS);
  }
}
