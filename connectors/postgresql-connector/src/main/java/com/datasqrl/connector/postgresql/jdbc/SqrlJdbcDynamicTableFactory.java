/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.connector.postgresql.jdbc;

import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.DRIVER;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_MISSING_KEY;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.MAX_RETRY_TIMEOUT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_AUTO_COMMIT;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_FETCH_SIZE;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_COLUMN;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_LOWER_BOUND;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_NUM;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SCAN_PARTITION_UPPER_BOUND;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_MAX_RETRIES;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.USERNAME;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialectLoader;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSink;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSource;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * Factory for creating configured instances of {@link JdbcDynamicTableSource} and {@link
 * JdbcDynamicTableSink}.
 */
@Internal
@AutoService(Factory.class)
public class SqrlJdbcDynamicTableFactory implements DynamicTableSinkFactory {

  public static final String IDENTIFIER = "jdbc-sqrl";

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    final var helper = FactoryUtil.createTableFactoryHelper(this, context);
    final var config = helper.getOptions();

    helper.validate();
    validateConfigOptions(config, context.getClassLoader());
    validateDataTypeWithJdbcDialect(
        context.getPhysicalRowDataType(), config.get(URL), context.getClassLoader());
    var jdbcOptions = getJdbcOptions(config, context.getClassLoader());

    return new JdbcDynamicTableSink(
        jdbcOptions,
        getJdbcExecutionOptions(config),
        getJdbcDmlOptions(
            jdbcOptions, context.getPhysicalRowDataType(), context.getPrimaryKeyIndexes()),
        context.getPhysicalRowDataType());
  }

  private static void validateDataTypeWithJdbcDialect(
      DataType dataType, String url, ClassLoader classLoader) {
    var dialect = loadDialect(url, classLoader);

    dialect.validate((RowType) dataType.getLogicalType());
  }

  private InternalJdbcConnectionOptions getJdbcOptions(
      ReadableConfig readableConfig, ClassLoader classLoader) {
    final var url = readableConfig.get(URL);
    final var builder =
        InternalJdbcConnectionOptions.builder()
            .setClassLoader(classLoader)
            .setDBUrl(url)
            .setTableName(readableConfig.get(TABLE_NAME))
            .setDialect(loadDialect(url, classLoader))
            .setParallelism(readableConfig.getOptional(SINK_PARALLELISM).orElse(null))
            .setConnectionCheckTimeoutSeconds(
                (int) readableConfig.get(MAX_RETRY_TIMEOUT).getSeconds());

    readableConfig.getOptional(DRIVER).ifPresent(builder::setDriverName);
    readableConfig.getOptional(USERNAME).ifPresent(builder::setUsername);
    readableConfig.getOptional(PASSWORD).ifPresent(builder::setPassword);
    return builder.build();
  }

  private static JdbcDialect loadDialect(String url, ClassLoader classLoader) {
    var dialect = JdbcDialectLoader.load(url, classLoader);
    // sqrl: standard postgres dialect with extended dialect
    if (dialect.dialectName().equalsIgnoreCase("PostgreSQL")) {
      return new SqrlPostgresDialect();
    }
    return dialect;
  }

  private JdbcExecutionOptions getJdbcExecutionOptions(ReadableConfig config) {
    final var builder = new JdbcExecutionOptions.Builder();
    builder.withBatchSize(config.get(SINK_BUFFER_FLUSH_MAX_ROWS));
    builder.withBatchIntervalMs(config.get(SINK_BUFFER_FLUSH_INTERVAL).toMillis());
    builder.withMaxRetries(config.get(SINK_MAX_RETRIES));
    return builder.build();
  }

  private JdbcDmlOptions getJdbcDmlOptions(
      InternalJdbcConnectionOptions jdbcOptions, DataType dataType, int[] primaryKeyIndexes) {

    var keyFields =
        Arrays.stream(primaryKeyIndexes)
            .mapToObj(i -> DataType.getFieldNames(dataType).get(i))
            .toArray(String[]::new);

    return JdbcDmlOptions.builder()
        .withTableName(jdbcOptions.getTableName())
        .withDialect(jdbcOptions.getDialect())
        .withFieldNames(DataType.getFieldNames(dataType).toArray(new String[0]))
        .withKeyFields(keyFields.length > 0 ? keyFields : null)
        .build();
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> requiredOptions = new HashSet<>();
    requiredOptions.add(URL);
    requiredOptions.add(TABLE_NAME);
    return requiredOptions;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> optionalOptions = new HashSet<>();
    optionalOptions.add(DRIVER);
    optionalOptions.add(USERNAME);
    optionalOptions.add(PASSWORD);
    optionalOptions.add(SCAN_PARTITION_COLUMN);
    optionalOptions.add(SCAN_PARTITION_LOWER_BOUND);
    optionalOptions.add(SCAN_PARTITION_UPPER_BOUND);
    optionalOptions.add(SCAN_PARTITION_NUM);
    optionalOptions.add(SCAN_FETCH_SIZE);
    optionalOptions.add(SCAN_AUTO_COMMIT);
    optionalOptions.add(LOOKUP_CACHE_MAX_ROWS);
    optionalOptions.add(LOOKUP_CACHE_TTL);
    optionalOptions.add(LOOKUP_MAX_RETRIES);
    optionalOptions.add(LOOKUP_CACHE_MISSING_KEY);
    optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
    optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
    optionalOptions.add(SINK_MAX_RETRIES);
    optionalOptions.add(SINK_PARALLELISM);
    optionalOptions.add(MAX_RETRY_TIMEOUT);
    optionalOptions.add(LookupOptions.CACHE_TYPE);
    optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS);
    optionalOptions.add(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE);
    optionalOptions.add(LookupOptions.PARTIAL_CACHE_MAX_ROWS);
    optionalOptions.add(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
    optionalOptions.add(LookupOptions.MAX_RETRIES);
    return optionalOptions;
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return Stream.of(
            URL,
            TABLE_NAME,
            USERNAME,
            PASSWORD,
            DRIVER,
            SINK_BUFFER_FLUSH_MAX_ROWS,
            SINK_BUFFER_FLUSH_INTERVAL,
            SINK_MAX_RETRIES,
            MAX_RETRY_TIMEOUT,
            SCAN_FETCH_SIZE,
            SCAN_AUTO_COMMIT)
        .collect(Collectors.toSet());
  }

  private void validateConfigOptions(ReadableConfig config, ClassLoader classLoader) {
    var jdbcUrl = config.get(URL);
    //        JdbcDialectLoader.load(jdbcUrl, classLoader);

    checkAllOrNone(config, new ConfigOption[] {USERNAME, PASSWORD});

    checkAllOrNone(
        config,
        new ConfigOption[] {
          SCAN_PARTITION_COLUMN,
          SCAN_PARTITION_NUM,
          SCAN_PARTITION_LOWER_BOUND,
          SCAN_PARTITION_UPPER_BOUND
        });

    if (config.getOptional(SCAN_PARTITION_LOWER_BOUND).isPresent()
        && config.getOptional(SCAN_PARTITION_UPPER_BOUND).isPresent()) {
      long lowerBound = config.get(SCAN_PARTITION_LOWER_BOUND);
      long upperBound = config.get(SCAN_PARTITION_UPPER_BOUND);
      if (lowerBound > upperBound) {
        throw new IllegalArgumentException(
            String.format(
                "'%s'='%s' must not be larger than '%s'='%s'.",
                SCAN_PARTITION_LOWER_BOUND.key(),
                lowerBound,
                SCAN_PARTITION_UPPER_BOUND.key(),
                upperBound));
      }
    }

    checkAllOrNone(config, new ConfigOption[] {LOOKUP_CACHE_MAX_ROWS, LOOKUP_CACHE_TTL});

    if (config.get(LOOKUP_MAX_RETRIES) < 0) {
      throw new IllegalArgumentException(
          String.format(
              "The value of '%s' option shouldn't be negative, but is %s.",
              LOOKUP_MAX_RETRIES.key(), config.get(LOOKUP_MAX_RETRIES)));
    }

    if (config.get(SINK_MAX_RETRIES) < 0) {
      throw new IllegalArgumentException(
          String.format(
              "The value of '%s' option shouldn't be negative, but is %s.",
              SINK_MAX_RETRIES.key(), config.get(SINK_MAX_RETRIES)));
    }

    if (config.get(MAX_RETRY_TIMEOUT).getSeconds() <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "The value of '%s' option must be in second granularity and shouldn't be smaller than 1 second, but is %s.",
              MAX_RETRY_TIMEOUT.key(),
              config.get(
                  ConfigOptions.key(MAX_RETRY_TIMEOUT.key()).stringType().noDefaultValue())));
    }
  }

  private void checkAllOrNone(ReadableConfig config, ConfigOption<?>[] configOptions) {
    var presentCount = 0;
    for (ConfigOption<?> configOption : configOptions) {
      if (config.getOptional(configOption).isPresent()) {
        presentCount++;
      }
    }
    var propertyNames = Arrays.stream(configOptions).map(ConfigOption::key).toArray(String[]::new);
    Preconditions.checkArgument(
        configOptions.length == presentCount || presentCount == 0,
        "Either all or none of the following options should be provided:\n"
            + String.join("\n", propertyNames));
  }
}
