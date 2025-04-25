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
package com.datasqrl.flinkrunner.functions.json;

import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.strategies.SpecificInputTypeStrategies;

public class JsonFunctions {

  public static final to_jsonb TO_JSON = new to_jsonb();
  public static final jsonb_to_string JSON_TO_STRING = new jsonb_to_string();
  public static final jsonb_object JSON_OBJECT = new jsonb_object();
  public static final jsonb_array JSON_ARRAY = new jsonb_array();
  public static final jsonb_extract JSON_EXTRACT = new jsonb_extract();
  public static final jsonb_query JSON_QUERY = new jsonb_query();
  public static final jsonb_exists JSON_EXISTS = new jsonb_exists();
  public static final jsonb_array_agg JSON_ARRAYAGG = new jsonb_array_agg();
  public static final jsonb_object_agg JSON_OBJECTAGG = new jsonb_object_agg();
  public static final jsonb_concat JSON_CONCAT = new jsonb_concat();

  public static ArgumentTypeStrategy createJsonArgumentTypeStrategy(DataTypeFactory typeFactory) {
    return InputTypeStrategies.or(
        SpecificInputTypeStrategies.JSON_ARGUMENT,
        InputTypeStrategies.explicit(createJsonType(typeFactory)));
  }

  public static DataType createJsonType(DataTypeFactory typeFactory) {
    var dataType = DataTypes.of(FlinkJsonType.class).toDataType(typeFactory);
    return dataType;
  }
}
