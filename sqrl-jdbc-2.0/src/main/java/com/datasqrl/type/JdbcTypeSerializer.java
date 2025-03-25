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
package com.datasqrl.type;

import org.apache.flink.table.types.logical.LogicalType;

public interface JdbcTypeSerializer<D, S> {

  String getDialectId();

  Class getConversionClass();

  String dialectTypeName();

  GenericDeserializationConverter<D> getDeserializerConverter();

  GenericSerializationConverter<S> getSerializerConverter(LogicalType type);

  interface GenericSerializationConverter<T> {
    T create();
  }

  interface GenericDeserializationConverter<T> {
    T create();
  }
}
