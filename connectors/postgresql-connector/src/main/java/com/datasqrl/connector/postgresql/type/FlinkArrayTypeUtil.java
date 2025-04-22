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
package com.datasqrl.connector.postgresql.type;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkArrayTypeUtil {

  public static LogicalType getBaseFlinkArrayType(LogicalType type) {
    if (type instanceof ArrayType arrayType) {
      return getBaseFlinkArrayType(arrayType.getElementType());
    }
    return type;
  }

  public static boolean isScalarArray(LogicalType type) {
    if (type instanceof ArrayType arrayType) {
      var elementType = arrayType.getElementType();
      return isScalar(elementType) || isScalarArray(elementType);
    }
    return false;
  }

  public static boolean isScalar(LogicalType type) {
    return switch (type.getTypeRoot()) {
      case BOOLEAN,
              TINYINT,
              SMALLINT,
              INTEGER,
              BIGINT,
              FLOAT,
              DOUBLE,
              CHAR,
              VARCHAR,
              BINARY,
              VARBINARY,
              DATE,
              TIME_WITHOUT_TIME_ZONE,
              TIMESTAMP_WITH_TIME_ZONE,
              TIMESTAMP_WITHOUT_TIME_ZONE,
              TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              DECIMAL ->
          true;
      default -> false;
    };
  }
}
