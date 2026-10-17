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
package com.datasqrl.flinkrunner.connector.pinot;

import com.google.auto.service.AutoService;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * Flink Table factory for the {@code pinot} connector identifier.
 *
 * <p>Example SQRL / FlinkSQL usage:
 *
 * <pre>{@code
 * CREATE TABLE PinotSink (
 *   user_id   BIGINT,
 *   event_ts  TIMESTAMP_LTZ(3),
 *   action    STRING
 * ) WITH (
 *   'connector'           = 'pinot',
 *   'controller.url'      = 'http://pinot-controller:9000',
 *   'table.name'          = 'user_events_OFFLINE',
 *   'segment.flush.rows'  = '200000'
 * );
 *
 * EXPORT user_events -> PinotSink;
 * }</pre>
 */
@AutoService(Factory.class)
public class PinotDynamicTableFactory implements DynamicTableSinkFactory {

  public static final String IDENTIFIER = "pinot";

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(PinotOptions.CONTROLLER_URL, PinotOptions.TABLE_NAME);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(PinotOptions.SEGMENT_FLUSH_ROWS);
  }

  @Override
  public DynamicTableSink createDynamicTableSink(Context context) {
    var helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();
    var options = helper.getOptions();

    return new PinotDynamicTableSink(
        options.get(PinotOptions.CONTROLLER_URL),
        options.get(PinotOptions.TABLE_NAME),
        options.get(PinotOptions.SEGMENT_FLUSH_ROWS),
        context.getPhysicalRowDataType());
  }
}
