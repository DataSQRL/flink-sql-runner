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
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.SCAN_BOUNDED;
import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.SOURCE_CLASS;

import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
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

    UserSource source = UserSource.load(className, context.getClassLoader());
    CamelConnectorOptions.validateFor(source.mode(), tableOptions.keySet(), className);
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

    return new CamelDynamicTableSource(
        source,
        recordType,
        context.getPhysicalRowDataType(),
        options,
        componentOptions(tableOptions),
        options.get(SCAN_BOUNDED) || batch);
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
