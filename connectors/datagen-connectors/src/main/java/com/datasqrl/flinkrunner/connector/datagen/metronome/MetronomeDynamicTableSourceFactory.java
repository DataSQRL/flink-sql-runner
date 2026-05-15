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
package com.datasqrl.flinkrunner.connector.datagen.metronome;

import com.google.auto.service.AutoService;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/** Dynamic table factory for the {@code metronome} connector. */
@AutoService(Factory.class)
public class MetronomeDynamicTableSourceFactory implements DynamicTableSourceFactory {

  public static final String IDENTIFIER = "metronome";

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    var helper = FactoryUtil.createTableFactoryHelper(this, context);
    helper.validate();

    var rowDataType = context.getPhysicalRowDataType();
    validateSchema(rowDataType);

    return new MetronomeTableSource(rowDataType);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of();
  }

  private static void validateSchema(DataType rowDataType) {
    var fieldDataTypes = DataType.getFieldDataTypes(rowDataType);
    if (fieldDataTypes.size() != 2) {
      throw new ValidationException(
          "Metronome source expects exactly two physical columns: BIGINT sequence number and timestamp.");
    }

    var firstField = fieldDataTypes.get(0).getLogicalType().getTypeRoot();
    var secondField = fieldDataTypes.get(1).getLogicalType().getTypeRoot();

    if (firstField != LogicalTypeRoot.BIGINT) {
      throw new ValidationException("Metronome source first column must be BIGINT.");
    }

    if (secondField != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE
        && secondField != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
      throw new ValidationException(
          "Metronome source second column must be TIMESTAMP or TIMESTAMP_LTZ.");
    }
  }
}
