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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class PollScheduleTest {

  private static final long NOW = Instant.parse("2026-11-20T10:15:30Z").toEpochMilli();

  @Test
  void intervalPollsThatLongAfterNow() {
    assertThat(PollSchedule.every(Duration.ofMinutes(5)).nextPollAt(NOW))
        .isEqualTo(NOW + Duration.ofMinutes(5).toMillis());
  }

  @Test
  void cronPollsAtTheNextMatchingTimeInItsZone() {
    PollSchedule daily = PollSchedule.cron("0 0 6 * * ?", ZoneId.of("America/Vancouver"));

    // 10:15 UTC is 02:15 in Vancouver, so the next 06:00 there is 14:00 UTC.
    assertThat(daily.nextPollAt(NOW))
        .isEqualTo(Instant.parse("2026-11-20T14:00:00Z").toEpochMilli());
  }

  @Test
  void cronAtAMatchingTimePollsAtTheNextOne() {
    PollSchedule everyMinute = PollSchedule.cron("0 * * * * ?", ZoneOffset.UTC);
    long onTheMinute = Instant.parse("2026-11-20T10:15:00Z").toEpochMilli();

    assertThat(everyMinute.nextPollAt(onTheMinute))
        .isEqualTo(onTheMinute + Duration.ofMinutes(1).toMillis());
  }

  @Test
  void rejectsAnInvalidCron() {
    assertThatThrownBy(() -> PollSchedule.cron("every day", ZoneOffset.UTC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("every day");
  }

  @Test
  void rejectsANonPositiveInterval() {
    assertThatThrownBy(() -> PollSchedule.every(Duration.ZERO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
  }
}
