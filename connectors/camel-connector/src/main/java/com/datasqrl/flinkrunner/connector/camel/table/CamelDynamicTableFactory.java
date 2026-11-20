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

import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.COMPONENT_PREFIX;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.CURSOR_START;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.POLL_CRON;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.POLL_INTERVAL;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.SCAN_BOUNDED;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.SOURCE_CLASS;

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.Bind;
import com.datasqrl.flinkrunner.camel.FileSource;
import com.datasqrl.flinkrunner.connector.camel.source.CamelClasspath;
import com.datasqrl.flinkrunner.connector.camel.source.polling.PollSchedule;
import com.google.auto.service.AutoService;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

/**
 * Creates {@link CamelDynamicTableSource}s for tables with {@code 'connector' = 'camel'}. The class
 * named in {@code source.class} is loaded from the job's classpath; the contract it implements is
 * the mode, and its record's components are the physical columns.
 *
 * <pre>{@code
 * CREATE TABLE accounts (
 *   id STRING,
 *   modifiedAt TIMESTAMP_LTZ(3),
 *   ingested_at TIMESTAMP_LTZ(3) METADATA,
 *   WATERMARK FOR ingested_at AS SOURCE_WATERMARK()
 * ) WITH (
 *   'connector' = 'camel',
 *   'source.class' = 'com.example.Accounts'
 * );
 * }</pre>
 */
@AutoService(Factory.class)
public final class CamelDynamicTableFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "camel";

  private static final char COMPONENT_SEPARATOR = '.';

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(SOURCE_CLASS);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return CamelConnectorOptions.optionalOptions();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validateExcept(COMPONENT_PREFIX);
    ReadableConfig options = helper.getOptions();
    Map<String, String> tableOptions = context.getCatalogTable().getOptions();
    String className = options.get(SOURCE_CLASS);

    ClassLoader classLoader = context.getClassLoader();
    // Before the source class loads: a Camel jar of another version can fail its linking.
    requireConnectorsCamelVersion(classLoader);
    UserSource source = UserSource.load(className, classLoader);
    CamelConnectorOptions.validateFor(source.mode(), tableOptions.keySet(), className);
    Map<String, String> componentOptions = componentOptions(tableOptions);
    requireComponents(source, className, componentOptions.keySet(), classLoader);
    if (context.getCatalogTable().getResolvedSchema().getPrimaryKey().isPresent()) {
      throw new ValidationException(
          "Camel source tables are append-only and declare no primary key. Declare keys"
              + " downstream, where duplicates are resolved.");
    }
    DataType recordType = RecordType.bridge(source.recordClass(), context.getPhysicalRowDataType());

    boolean batch =
        context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH;
    // Without checkpoints there is no safe point to acknowledge, so every message would be
    // redelivered on the next run. A bounded source in a streaming job acks on its final
    // checkpoint instead.
    if (batch && source.mode() == SourceMode.ACK) {
      throw new ValidationException(
          "Ack source '"
              + className
              + "' acknowledges messages on checkpoints, which batch mode does not take. Run in"
              + " streaming mode and set '"
              + SCAN_BOUNDED.key()
              + "' for a bounded read.");
    }

    Object cursorStart =
        options
            .getOptional(CURSOR_START)
            .map(
                text ->
                    CursorStart.parse(
                        text, CursorStart.positionClass(source.instance().getClass())))
            .orElse(null);

    return new CamelDynamicTableSource(
        source,
        recordType,
        context.getPhysicalRowDataType(),
        options,
        componentOptions,
        options.get(SCAN_BOUNDED) || batch,
        pollSchedule(options, context.getConfiguration()),
        cursorStart);
  }

  private static void requireConnectorsCamelVersion(ClassLoader classLoader) {
    List<String> others = CamelClasspath.otherCamelVersions(classLoader);
    if (!others.isEmpty()) {
      throw new ValidationException(
          "The job's classpath has Camel jars of another version than "
              + CamelClasspath.CAMEL_VERSION
              + ", the one the connector is built against: "
              + String.join(", ", others)
              + ". Camel needs all its jars at one version: take every Camel component at "
              + CamelClasspath.CAMEL_VERSION
              + ", e.g. by importing org.apache.camel:camel-bom:"
              + CamelClasspath.CAMEL_VERSION
              + ".");
    }
  }

  /**
   * The components the {@code camel.component.*} options configure, and the one the source's
   * endpoint or location names, must be on the classpath or bound by the source. Cursor requests
   * are only built on the fetcher thread, so the runtime checks those.
   */
  private static void requireComponents(
      UserSource source,
      String className,
      Set<String> componentOptionKeys,
      ClassLoader classLoader) {
    Set<String> bound = boundNames(source.instance());
    for (String key : componentOptionKeys) {
      int separator = key.indexOf(COMPONENT_SEPARATOR);
      if (separator > 0) {
        String name = key.substring(0, separator);
        requireComponent(
            name, "table options '" + COMPONENT_PREFIX + name + ".*'", bound, classLoader);
      }
    }
    endpoint(source)
        .flatMap(CamelClasspath::scheme)
        .ifPresent(
            scheme ->
                requireComponent(scheme, "Camel source '" + className + "'", bound, classLoader));
  }

  private static void requireComponent(
      String name, String user, Set<String> bound, ClassLoader classLoader) {
    if (!bound.contains(name) && !CamelClasspath.hasComponent(name, classLoader)) {
      throw new ValidationException(CamelClasspath.missingComponent(name, user));
    }
  }

  /** The names the source's {@link Bind} methods register, components among them. */
  private static Set<String> boundNames(Object source) {
    return Arrays.stream(source.getClass().getMethods())
        .map(method -> method.getAnnotation(Bind.class))
        .filter(Objects::nonNull)
        .map(Bind::value)
        .collect(Collectors.toSet());
  }

  private static Optional<String> endpoint(UserSource source) {
    return switch (source.mode()) {
      case ACK -> Optional.ofNullable(((AckSource<?>) source.instance()).endpoint());
      case FILE -> Optional.ofNullable(((FileSource<?, ?>) source.instance()).location());
      case CURSOR -> Optional.empty();
    };
  }

  /** {@code poll.cron} in the session's time zone when set, otherwise {@code poll.interval}. */
  private static PollSchedule pollSchedule(ReadableConfig options, ReadableConfig session) {
    Optional<String> cron = options.getOptional(POLL_CRON);
    if (cron.isPresent() && options.getOptional(POLL_INTERVAL).isPresent()) {
      throw new ValidationException(
          "Set either '" + POLL_INTERVAL.key() + "' or '" + POLL_CRON.key() + "', not both.");
    }
    try {
      return cron.isPresent()
          ? PollSchedule.cron(cron.get(), localTimeZone(session))
          : PollSchedule.every(options.get(POLL_INTERVAL));
    } catch (IllegalArgumentException e) {
      throw new ValidationException("Invalid poll schedule: " + e.getMessage(), e);
    }
  }

  /** The session's {@code table.local-time-zone}, resolved the way Flink resolves it. */
  private static ZoneId localTimeZone(ReadableConfig session) {
    String zone = session.get(TableConfigOptions.LOCAL_TIME_ZONE);
    return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
        ? ZoneId.systemDefault()
        : ZoneId.of(zone);
  }

  /** The {@code camel.component.*} options, without the prefix. */
  private static Map<String, String> componentOptions(Map<String, String> tableOptions) {
    Map<String, String> componentOptions = new HashMap<>();
    tableOptions.forEach(
        (key, value) -> {
          if (key.startsWith(COMPONENT_PREFIX)) {
            componentOptions.put(key.substring(COMPONENT_PREFIX.length()), value);
          }
        });
    return componentOptions;
  }
}
