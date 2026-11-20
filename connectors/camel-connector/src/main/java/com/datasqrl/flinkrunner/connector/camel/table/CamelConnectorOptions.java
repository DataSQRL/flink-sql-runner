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

import static com.datasqrl.flinkrunner.connector.camel.table.SourceMode.ACK;
import static com.datasqrl.flinkrunner.connector.camel.table.SourceMode.CURSOR;
import static com.datasqrl.flinkrunner.connector.camel.table.SourceMode.FILE;

import com.datasqrl.flinkrunner.connector.camel.source.file.CamelFileSource;
import java.time.Duration;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;

/** Options of the {@code camel} table connector, and the source modes each applies to. */
public final class CamelConnectorOptions {

  /** Options with this prefix configure a Camel component, e.g. {@code camel.component.jms.x}. */
  public static final String COMPONENT_PREFIX = "camel.component.";

  /** {@code cursor.reset}. */
  public enum CursorReset {
    NEVER("never"),
    PER_POLL("per-poll");

    private final String value;

    CursorReset(String value) {
      this.value = value;
    }

    // Flink matches enum option values against toString().
    @Override
    public String toString() {
      return value;
    }
  }

  public static final ConfigOption<String> SOURCE_CLASS =
      ConfigOptions.key("source.class")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The class implementing CursorSource, AckSource or FileSource. Its contract is the"
                  + " mode, its output record the table's columns.");

  public static final ConfigOption<Duration> FETCH_TIMEOUT =
      ConfigOptions.key("fetch.timeout")
          .durationType()
          .defaultValue(Duration.ofSeconds(1))
          .withDescription(
              "How long one Camel receive waits, and how long the fetcher waits at most between"
                  + " checks while idle or backing off.");

  public static final ConfigOption<Integer> RETRY_MAX_ATTEMPTS =
      ConfigOptions.key("retry.max-attempts")
          .intType()
          .defaultValue(10)
          .withDescription(
              "Failed fetches in a row before the job fails. 0 turns retries off: a failed request"
                  + " then fails the job straight away.");

  public static final ConfigOption<Duration> RETRY_INITIAL_BACKOFF =
      ConfigOptions.key("retry.initial-backoff")
          .durationType()
          .defaultValue(Duration.ofMinutes(1))
          .withDescription("Delay before the first retry; it doubles on every failure.");

  public static final ConfigOption<Duration> RETRY_MAX_BACKOFF =
      ConfigOptions.key("retry.max-backoff")
          .durationType()
          .defaultValue(Duration.ofMinutes(30))
          .withDescription("Upper bound of the delay between retries.");

  public static final ConfigOption<Boolean> SCAN_BOUNDED =
      ConfigOptions.key("scan.bounded")
          .booleanType()
          .defaultValue(false)
          .withDescription(
              "Makes the source bounded: cursor and file sources finish after one complete poll,"
                  + " ack sources after 'ack.empty-polls' empty receives in a row.");

  public static final ConfigOption<Duration> POLL_INTERVAL =
      ConfigOptions.key("poll.interval")
          .durationType()
          .defaultValue(Duration.ofMinutes(5))
          .withDescription("Time between polls once a poll is complete.");

  public static final ConfigOption<String> POLL_CRON =
      ConfigOptions.key("poll.cron")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "A Quartz cron expression, such as '0 0 6 * * ?', for when polls start, in"
                  + " 'table.local-time-zone'. Replaces 'poll.interval'.");

  public static final ConfigOption<Integer> RATE_LIMIT =
      ConfigOptions.key("rate-limit")
          .intType()
          .noDefaultValue()
          .withDescription("Max Camel requests or listings per minute. No limit when not set.");

  public static final ConfigOption<String> CURSOR_START =
      ConfigOptions.key("cursor.start")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The position to start from instead of initial(), parsed by the position type's"
                  + " static parse or valueOf method. A restored position takes precedence.");

  public static final ConfigOption<CursorReset> CURSOR_RESET =
      ConfigOptions.key("cursor.reset")
          .enumType(CursorReset.class)
          .defaultValue(CursorReset.NEVER)
          .withDescription(
              "'per-poll' pages through everything from the start position on every poll: a full"
                  + " refresh, for snapshot reconciliation.");

  public static final ConfigOption<Duration> CURSOR_LOOKBACK =
      ConfigOptions.key("cursor.lookback")
          .durationType()
          .defaultValue(Duration.ZERO)
          .withDescription(
              "Starts every request this far before the position, to read rows that concurrent"
                  + " writers committed late.");

  public static final ConfigOption<Integer> ACK_MAX_PENDING =
      ConfigOptions.key("ack.max-pending")
          .intType()
          .defaultValue(10_000)
          .withDescription("Max messages held while waiting for a checkpoint.");

  public static final ConfigOption<Integer> ACK_EMPTY_POLLS =
      ConfigOptions.key("ack.empty-polls")
          .intType()
          .defaultValue(3)
          .withDescription(
              "With 'scan.bounded', the source finishes after this many receives in a row return"
                  + " nothing.");

  public static final ConfigOption<String> FILE_READY =
      ConfigOptions.key("file.ready")
          .stringType()
          .defaultValue("stable-size")
          .withDescription(
              "When a listed file is ready: 'stable-size', 'trigger:<suffix>' such as"
                  + " 'trigger:.done', or 'none'.");

  public static final ConfigOption<CamelFileSource.Order> FILE_ORDER =
      ConfigOptions.key("file.order")
          .enumType(CamelFileSource.Order.class)
          .defaultValue(CamelFileSource.Order.NAME)
          .withDescription("The order files are read in: 'name', 'mtime' or 'none'.");

  public static final ConfigOption<CamelFileSource.SnapshotUnit> FILE_SNAPSHOT_UNIT =
      ConfigOptions.key("file.snapshot-unit")
          .enumType(CamelFileSource.SnapshotUnit.class)
          .defaultValue(CamelFileSource.SnapshotUnit.FILE)
          .withDescription(
              "What one snapshot is: 'file' (each new file stands alone) or 'listing' (all"
                  + " matching files together).");

  public static final ConfigOption<Duration> FILE_SEEN_RETENTION =
      ConfigOptions.key("file.seen-retention")
          .durationType()
          .defaultValue(Duration.ofDays(30))
          .withDescription("Forgets a file version once no listing has contained it this long.");

  public static final ConfigOption<Double> FILE_MIN_ROW_RATIO =
      ConfigOptions.key("file.min-row-ratio")
          .doubleType()
          .noDefaultValue()
          .withDescription(
              "Fails the job when a unit has fewer rows than this ratio of the previous one's.");

  private static final Set<SourceMode> ALL = EnumSet.allOf(SourceMode.class);
  private static final Set<SourceMode> POLLING = EnumSet.of(CURSOR, FILE);

  private static final Map<ConfigOption<?>, Set<SourceMode>> MODES = modes();

  private CamelConnectorOptions() {}

  /** Every option besides {@link #SOURCE_CLASS} and the component options. */
  static Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new LinkedHashSet<>(MODES.keySet());
    options.remove(SOURCE_CLASS);
    return options;
  }

  /**
   * @throws ValidationException if a key the table sets belongs to an option of another mode
   */
  static void validateFor(SourceMode mode, Set<String> setKeys, String sourceClass) {
    for (Map.Entry<ConfigOption<?>, Set<SourceMode>> option : MODES.entrySet()) {
      String key = option.getKey().key();
      if (setKeys.contains(key) && !option.getValue().contains(mode)) {
        throw new ValidationException(
            "Option '"
                + key
                + "' does not apply to "
                + mode
                + " source '"
                + sourceClass
                + "', only to "
                + option.getValue()
                + " sources.");
      }
    }
  }

  private static Map<ConfigOption<?>, Set<SourceMode>> modes() {
    Map<ConfigOption<?>, Set<SourceMode>> modes = new LinkedHashMap<>();
    modes.put(SOURCE_CLASS, ALL);
    modes.put(FETCH_TIMEOUT, ALL);
    modes.put(RETRY_MAX_ATTEMPTS, ALL);
    modes.put(RETRY_INITIAL_BACKOFF, ALL);
    modes.put(RETRY_MAX_BACKOFF, ALL);
    modes.put(SCAN_BOUNDED, ALL);
    modes.put(POLL_INTERVAL, POLLING);
    modes.put(POLL_CRON, POLLING);
    modes.put(RATE_LIMIT, POLLING);
    modes.put(CURSOR_START, EnumSet.of(CURSOR));
    modes.put(CURSOR_RESET, EnumSet.of(CURSOR));
    modes.put(CURSOR_LOOKBACK, EnumSet.of(CURSOR));
    modes.put(ACK_MAX_PENDING, EnumSet.of(ACK));
    modes.put(ACK_EMPTY_POLLS, EnumSet.of(ACK));
    modes.put(FILE_READY, EnumSet.of(FILE));
    modes.put(FILE_ORDER, EnumSet.of(FILE));
    modes.put(FILE_SNAPSHOT_UNIT, EnumSet.of(FILE));
    modes.put(FILE_SEEN_RETENTION, EnumSet.of(FILE));
    modes.put(FILE_MIN_ROW_RATIO, EnumSet.of(FILE));
    return modes;
  }
}
