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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * Matches a source's record type to the table's columns and builds the structured type Flink
 * converts the records with. The columns must be the record's components, by name and in order, so
 * a mismatch fails at planning, never at runtime.
 */
final class RecordType {

  private static final int LIST_ELEMENT = 0;
  private static final int MAP_KEY = 0;
  private static final int MAP_VALUE = 1;

  private RecordType() {}

  /**
   * A {@code STRUCTURED} type over {@code recordClass} whose fields follow the row type's order,
   * each bridged to its component's class.
   *
   * @throws ValidationException if a column has no component, a component no column, or a column
   *     type cannot hold its component's Java type
   */
  static DataType bridge(Class<?> recordClass, DataType rowType) {
    Map<String, RecordComponent> components = new LinkedHashMap<>();
    for (RecordComponent component : recordClass.getRecordComponents()) {
      components.put(component.getName(), component);
    }
    List<String> columns = DataType.getFieldNames(rowType);
    List<DataType> columnTypes = DataType.getFieldDataTypes(rowType);
    for (String component : components.keySet()) {
      if (!columns.contains(component)) {
        throw new ValidationException(
            "Record component '"
                + component
                + "' of "
                + recordClass.getName()
                + " has no column. Declare every component of the record as a column.");
      }
    }
    List<String> componentNames = List.copyOf(components.keySet());
    DataTypes.Field[] fields = new DataTypes.Field[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      String column = columns.get(i);
      RecordComponent component = components.get(column);
      if (component == null) {
        throw new ValidationException(
            "Column '"
                + column
                + "' is not a component of "
                + recordClass.getName()
                + ", whose components are "
                + components.keySet());
      }
      // Flink builds records with their canonical constructor, which needs the components' order.
      if (!column.equals(componentNames.get(i))) {
        throw new ValidationException(
            "Column '"
                + column
                + "' is out of order. Declare the columns in the order of the components of "
                + recordClass.getName()
                + ": "
                + componentNames);
      }
      fields[i] =
          DataTypes.FIELD(
              column,
              bridgeColumn(
                  columnTypes.get(i),
                  component.getGenericType(),
                  recordClass.getSimpleName() + "." + column));
    }
    return DataTypes.STRUCTURED(recordClass, fields);
  }

  private static DataType bridgeColumn(DataType column, Type javaType, String path) {
    LogicalType logical = column.getLogicalType();
    Class<?> javaClass = rawClass(javaType);
    LogicalTypeRoot root = logical.getTypeRoot();
    if (root == LogicalTypeRoot.ROW && javaClass.isRecord()) {
      return nullability(bridge(javaClass, column), logical);
    }
    if (root == LogicalTypeRoot.ARRAY && List.class.equals(javaClass)) {
      DataType element =
          bridgeColumn(
              column.getChildren().get(0), typeArgument(javaType, LIST_ELEMENT), path + "[]");
      return nullability(DataTypes.ARRAY(element), logical).bridgedTo(List.class);
    }
    if (root == LogicalTypeRoot.MAP && Map.class.equals(javaClass)) {
      DataType key =
          bridgeColumn(column.getChildren().get(0), typeArgument(javaType, MAP_KEY), path + "{}");
      DataType value =
          bridgeColumn(column.getChildren().get(1), typeArgument(javaType, MAP_VALUE), path + "{}");
      return nullability(DataTypes.MAP(key, value), logical).bridgedTo(Map.class);
    }
    if (root != LogicalTypeRoot.ROW
        && root != LogicalTypeRoot.ARRAY
        && root != LogicalTypeRoot.MAP
        && logical.supportsInputConversion(javaClass)) {
      return column.bridgedTo(javaClass);
    }
    throw new ValidationException(
        "Column '"
            + path.substring(path.indexOf('.') + 1)
            + "' is "
            + logical.asSummaryString()
            + ", which cannot hold "
            + path
            + " of type "
            + javaType.getTypeName());
  }

  private static DataType nullability(DataType type, LogicalType like) {
    return like.isNullable() ? type.nullable() : type.notNull();
  }

  private static Class<?> rawClass(Type type) {
    if (type instanceof Class<?> plain) {
      return plain;
    }
    if (type instanceof ParameterizedType parameterized) {
      return (Class<?>) parameterized.getRawType();
    }
    throw new ValidationException("Unsupported record component type " + type.getTypeName());
  }

  private static Type typeArgument(Type type, int index) {
    if (type instanceof ParameterizedType parameterized) {
      return parameterized.getActualTypeArguments()[index];
    }
    throw new ValidationException(
        "Raw collection type " + type.getTypeName() + " is not supported");
  }
}
