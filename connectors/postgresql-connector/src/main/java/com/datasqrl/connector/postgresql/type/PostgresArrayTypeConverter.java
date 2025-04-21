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

public class PostgresArrayTypeConverter {

  /** Return the base array type for flink type */
  public static String getArrayScalarName(LogicalType type) {
    return switch (type.getTypeRoot()) {
      case CHAR, VARCHAR -> "text";
      case BOOLEAN -> "boolean";
      case BINARY, VARBINARY -> "bytea";
      case DECIMAL -> "decimal";
      case TINYINT -> "smallint";
      case SMALLINT -> "smallint";
      case INTEGER -> "integer";
      case BIGINT -> "bigint";
      case FLOAT -> "real"; // PostgreSQL uses REAL for float
      case DOUBLE -> "double";
      case DATE -> "date";
      case TIME_WITHOUT_TIME_ZONE -> "time without time zone";
      case TIMESTAMP_WITHOUT_TIME_ZONE -> "timestamp without time zone";
      case TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "timestamptz";
      case INTERVAL_YEAR_MONTH -> "interval year to month";
      case INTERVAL_DAY_TIME -> "interval day to second";
      case NULL -> "void";
      case ARRAY -> getArrayScalarName(((ArrayType) type).getElementType());
      case MULTISET, MAP, ROW, DISTINCT_TYPE, STRUCTURED_TYPE, RAW, SYMBOL, UNRESOLVED ->
          throw new RuntimeException("Cannot convert type to array type");
      default -> throw new RuntimeException("Cannot convert type to array type");
    };
  }
}
