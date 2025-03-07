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

// import com.google.common.base.Preconditions;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;

/**
 * Time window function that returns the end of year for the timestamp argument. E.g.
 * endOfYear(parseTimestamp(2023-03-12T18:23:34.083Z)) returns the timestamp
 * 2023-12-31T23:59:59.999999999Z
 */
public class EndOfYear extends TimeTumbleWindowFunction {

  public EndOfYear() {
    super(ChronoUnit.YEARS, ChronoUnit.DAYS);
  }

  public Instant eval(Instant instant) {
    return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
        .with(TemporalAdjusters.firstDayOfNextYear())
        .truncatedTo(ChronoUnit.DAYS)
        .minusNanos(1)
        .toInstant();
  }

  public Instant eval(Instant instant, Long multiple, Long offset) {
    if (multiple == null) {
      multiple = 1L;
    }
    //    Preconditions.checkArgument(multiple > 0 && multiple < Integer.MAX_VALUE,
    //        "Window width must be a positive integer value: %s", multiple);
    if (offset == null) {
      offset = 0L;
    }
    //    Preconditions.checkArgument(offset >= 0 && offset < 365, "Invalid offset in days: %s",
    // offset);

    ZonedDateTime time =
        ZonedDateTime.ofInstant(instant, ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
    if (time.getDayOfYear() > offset) {
      time = time.with(TemporalAdjusters.firstDayOfNextYear());
    } else {
      time = time.with(TemporalAdjusters.firstDayOfYear());
    }
    int modulus = multiple.intValue();
    int yearsToAdd = (modulus - time.getYear() % modulus) % modulus;

    time = time.plusYears(yearsToAdd).plusDays(offset);
    return time.minusNanos(1).toInstant();
  }
}
