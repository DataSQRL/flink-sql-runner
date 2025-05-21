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
    switch (type.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return "text";
      case BOOLEAN:
        return "boolean";
      case BINARY:
      case VARBINARY:
        return "bytea";
      case DECIMAL:
        return "decimal";
      case TINYINT:
        return "smallint";
      case SMALLINT:
        return "smallint";
      case INTEGER:
        return "integer";
      case BIGINT:
        return "bigint";
      case FLOAT:
        return "real"; // PostgreSQL uses REAL for float
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME_WITHOUT_TIME_ZONE:
        return "time without time zone";
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return "timestamp without time zone";
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "timestamptz";
      case INTERVAL_YEAR_MONTH:
        return "interval year to month";
      case INTERVAL_DAY_TIME:
        return "interval day to second";
      case NULL:
        return "void";
      case ARRAY:
        return getArrayScalarName(((ArrayType) type).getElementType());
      case MULTISET:
      case MAP:
      case ROW:
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
      case RAW:
      case SYMBOL:
      case UNRESOLVED:
        throw new RuntimeException("Cannot convert type to array type");
      default:
        throw new RuntimeException("Cannot convert type to array type");
    }
  }
}
