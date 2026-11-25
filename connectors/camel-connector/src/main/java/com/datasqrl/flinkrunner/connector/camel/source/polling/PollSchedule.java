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

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.apache.flink.annotation.Internal;

/** When the next poll starts once a poll is complete: after an interval, or on a cron schedule. */
@Internal
public sealed interface PollSchedule extends Serializable {

  /** The time of the next poll after one that completed at {@code now}, in epoch milliseconds. */
  long nextPollAt(long now);

  /**
   * @throws IllegalArgumentException if the interval isn't positive
   */
  static PollSchedule every(Duration interval) {
    return new Every(interval);
  }

  /**
   * A Quartz cron expression, such as {@code 0 0 6 * * ?} for 06:00 every day, evaluated in {@code
   * zone}.
   *
   * @throws IllegalArgumentException if the expression isn't valid Quartz cron syntax
   */
  static PollSchedule cron(String expression, ZoneId zone) {
    return new Cron(expression, zone);
  }

  record Every(Duration interval) implements PollSchedule {

    public Every {
      if (interval == null || interval.isNegative() || interval.isZero()) {
        throw new IllegalArgumentException("The poll interval must be positive, got " + interval);
      }
    }

    @Override
    public long nextPollAt(long now) {
      return now + interval.toMillis();
    }
  }

  record Cron(String expression, ZoneId zone) implements PollSchedule {

    private static final CronParser QUARTZ =
        new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    /** A schedule without further matches, such as one limited to past years, never polls. */
    private static final long NEVER = Long.MAX_VALUE;

    public Cron {
      executionTime(expression);
    }

    @Override
    public long nextPollAt(long now) {
      return executionTime(expression)
          .nextExecution(ZonedDateTime.ofInstant(Instant.ofEpochMilli(now), zone))
          .map(next -> next.toInstant().toEpochMilli())
          .orElse(NEVER);
    }

    private static ExecutionTime executionTime(String expression) {
      try {
        return ExecutionTime.forCron(QUARTZ.parse(expression).validate());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "'" + expression + "' is not a Quartz cron expression: " + e.getMessage(), e);
      }
    }
  }
}
