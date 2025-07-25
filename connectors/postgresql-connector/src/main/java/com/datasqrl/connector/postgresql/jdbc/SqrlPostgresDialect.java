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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

/**
 * JDBC dialect for PostgreSQL.
 *
 * <p>SQRL: Add quoting to identifiers
 */
public class SqrlPostgresDialect extends AbstractDialect {

  private static final long serialVersionUID = -6927341136238028282L;
  // Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
  // https://www.postgresql.org/docs/12/datatype-datetime.html
  private static final int MAX_TIMESTAMP_PRECISION = 6;
  private static final int MIN_TIMESTAMP_PRECISION = 1;

  // Define MAX/MIN precision of DECIMAL type according to PostgreSQL docs:
  // https://www.postgresql.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
  private static final int MAX_DECIMAL_PRECISION = 1000;
  private static final int MIN_DECIMAL_PRECISION = 1;

  @Override
  public JdbcRowConverter getRowConverter(RowType rowType) {
    return new SqrlPostgresRowConverter(rowType);
  }

  @Override
  public String getLimitClause(long limit) {
    return "LIMIT " + limit;
  }

  @Override
  public Optional<String> defaultDriverName() {
    return Optional.of("org.postgresql.Driver");
  }

  /** Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres. */
  @Override
  public Optional<String> getUpsertStatement(
      String tableName, String[] fieldNames, String[] uniqueKeyFields) {
    var uniqueColumns =
        Arrays.stream(uniqueKeyFields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
    var updateClause =
        Arrays.stream(fieldNames)
            .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
            .collect(Collectors.joining(", "));
    return Optional.of(
        getInsertIntoStatement(tableName, fieldNames)
            + " ON CONFLICT ("
            + uniqueColumns
            + ")"
            + " DO UPDATE SET "
            + updateClause);
  }

  @Override
  public void validate(RowType rowType) throws ValidationException {
    List<LogicalType> unsupportedTypes =
        rowType.getFields().stream()
            .map(RowField::getType)
            .filter(type -> LogicalTypeRoot.RAW.equals(type.getTypeRoot()))
            .filter(type -> !isSupportedType(type))
            .collect(Collectors.toList());

    if (!unsupportedTypes.isEmpty()) {
      throw new ValidationException(
          String.format(
              "The %s dialect doesn't support type: %s.", this.dialectName(), unsupportedTypes));
    }

    super.validate(rowType);
  }

  private boolean isSupportedType(LogicalType type) {
    return SqrlPostgresRowConverter.sqrlSerializers.containsKey(type.getDefaultConversion());
  }

  @Override
  public String quoteIdentifier(String identifier) {
    return "\"" + identifier + "\"";
  }

  @Override
  public String dialectName() {
    return "PostgreSQL";
  }

  @Override
  public Optional<Range> decimalPrecisionRange() {
    return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
  }

  @Override
  public Optional<Range> timestampPrecisionRange() {
    return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
  }

  @Override
  public Set<LogicalTypeRoot> supportedTypes() {
    // The data types used in PostgreSQL are list at:
    // https://www.postgresql.org/docs/12/datatype.html

    // TODO: We can't convert BINARY data type to
    //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
    // LegacyTypeInfoDataTypeConverter.

    return EnumSet.of(
        LogicalTypeRoot.CHAR,
        LogicalTypeRoot.VARCHAR,
        LogicalTypeRoot.BOOLEAN,
        LogicalTypeRoot.VARBINARY,
        LogicalTypeRoot.DECIMAL,
        LogicalTypeRoot.TINYINT,
        LogicalTypeRoot.SMALLINT,
        LogicalTypeRoot.INTEGER,
        LogicalTypeRoot.BIGINT,
        LogicalTypeRoot.FLOAT,
        LogicalTypeRoot.DOUBLE,
        LogicalTypeRoot.DATE,
        LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
        LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
        LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        LogicalTypeRoot.ARRAY,
        LogicalTypeRoot.MAP,
        LogicalTypeRoot.RAW // see validate() for supported structured types
        );
  }
}
