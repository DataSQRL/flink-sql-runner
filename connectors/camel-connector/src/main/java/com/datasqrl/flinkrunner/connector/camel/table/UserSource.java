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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;

/**
 * The user's source class named in {@code source.class}, instantiated, with the mode its contract
 * gives and the record type it emits.
 */
record UserSource(Object instance, SourceMode mode, Class<?> recordClass) {

  private static final String CONTRACTS = "CursorSource, AckSource or FileSource";

  /**
   * @throws ValidationException if the class is missing, implements no contract or more than one,
   *     has no public no-argument constructor, or does not emit a record
   */
  static UserSource load(String className, ClassLoader classLoader) {
    Class<?> sourceClass;
    try {
      sourceClass = Class.forName(className, true, classLoader);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new ValidationException(
          "Camel source class '" + className + "' cannot be loaded from the job's classpath", e);
    }
    List<SourceMode> modes =
        Arrays.stream(SourceMode.values())
            .filter(mode -> mode.contract().isAssignableFrom(sourceClass))
            .toList();
    if (modes.isEmpty()) {
      throw new ValidationException(
          "Camel source class '" + className + "' must implement " + CONTRACTS);
    }
    if (modes.size() > 1) {
      throw new ValidationException(
          "Camel source class '"
              + className
              + "' must implement exactly one of "
              + CONTRACTS
              + ", but implements "
              + modes.stream()
                  .map(mode -> mode.contract().getSimpleName())
                  .collect(Collectors.joining(" and ")));
    }
    SourceMode mode = modes.get(0);
    Class<?> recordClass = recordClass(sourceClass, mode);
    return new UserSource(instantiate(sourceClass), mode, recordClass);
  }

  private static Class<?> recordClass(Class<?> sourceClass, SourceMode mode) {
    Type output = TypeExtractor.getParameterType(mode.contract(), sourceClass, 0);
    Class<?> outputClass =
        output instanceof Class<?> plain
            ? plain
            : output instanceof ParameterizedType parameterized
                ? (Class<?>) parameterized.getRawType()
                : null;
    if (outputClass == null || !outputClass.isRecord()) {
      throw new ValidationException(
          "The output type "
              + output.getTypeName()
              + " of Camel source class '"
              + sourceClass.getName()
              + "' must be a record, whose components become the table's columns");
    }
    return outputClass;
  }

  private static Object instantiate(Class<?> sourceClass) {
    try {
      return sourceClass.getConstructor().newInstance();
    } catch (NoSuchMethodException | IllegalAccessException e) {
      throw new ValidationException(
          "Camel source class '"
              + sourceClass.getName()
              + "' must be public and have a public no-argument constructor",
          e);
    } catch (InstantiationException | InvocationTargetException e) {
      throw new ValidationException(
          "Camel source class '" + sourceClass.getName() + "' failed to instantiate", e);
    }
  }
}
