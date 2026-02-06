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
package com.datasqrl.connector.postgresql.type;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkArrayTypeUtil {

  public static LogicalType getBaseFlinkArrayType(LogicalType type) {
    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      return getBaseFlinkArrayType(arrayType.getElementType());
    }
    return type;
  }

  public static boolean isScalarArray(LogicalType type) {
    if (type instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) type;
      LogicalType elementType = arrayType.getElementType();
      return isScalar(elementType) || isScalarArray(elementType);
    }
    return false;
  }

  public static boolean isScalar(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }
}
