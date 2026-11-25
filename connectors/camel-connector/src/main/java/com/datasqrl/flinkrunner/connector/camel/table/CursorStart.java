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

import static com.datasqrl.flinkrunner.connector.camel.table.CamelConnectorOptions.CURSOR_START;

import com.datasqrl.flinkrunner.camel.CursorSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;

/**
 * Turns the {@code cursor.start} text into a position. A {@code String} position is taken as is;
 * any other type needs a public static {@code parse} or {@code valueOf} method taking the text, as
 * {@code Instant}, {@code LocalDate}, {@code Long} and the like have, and a composite position
 * written for this.
 */
final class CursorStart {

  private static final List<String> PARSER_NAMES = List.of("parse", "valueOf");
  private static final List<Class<?>> PARSER_PARAMETERS = List.of(CharSequence.class, String.class);

  private CursorStart() {}

  /**
   * @throws ValidationException if the source class leaves the position type generic
   */
  static Class<?> positionClass(Class<?> sourceClass) {
    Type position = TypeExtractor.getParameterType(CursorSource.class, sourceClass, 1);
    if (position instanceof Class<?> plain) {
      return plain;
    }
    if (position instanceof ParameterizedType parameterized) {
      return (Class<?>) parameterized.getRawType();
    }
    throw new ValidationException(
        "'"
            + CURSOR_START.key()
            + "' needs a concrete position type, but Camel source class '"
            + sourceClass.getName()
            + "' leaves it as "
            + position.getTypeName());
  }

  /**
   * @throws ValidationException if the type has no parser, or the parser rejects the text
   */
  static Object parse(String text, Class<?> positionClass) {
    if (positionClass == String.class) {
      return text;
    }
    Method parser = parser(positionClass);
    try {
      return parser.invoke(null, text);
    } catch (InvocationTargetException e) {
      throw new ValidationException(
          "'"
              + CURSOR_START.key()
              + "' value '"
              + text
              + "' is not a valid "
              + positionClass.getName()
              + ": "
              + e.getCause().getMessage(),
          e.getCause());
    } catch (IllegalAccessException e) {
      throw new ValidationException("Cannot call " + parser, e);
    }
  }

  private static Method parser(Class<?> positionClass) {
    for (String name : PARSER_NAMES) {
      for (Class<?> parameter : PARSER_PARAMETERS) {
        try {
          Method method = positionClass.getMethod(name, parameter);
          if (Modifier.isStatic(method.getModifiers())
              && positionClass.isAssignableFrom(method.getReturnType())) {
            return method;
          }
        } catch (NoSuchMethodException e) {
          // Try the next signature.
        }
      }
    }
    throw new ValidationException(
        "'"
            + CURSOR_START.key()
            + "' needs a position type with a public static parse(String) or valueOf(String)"
            + " method, but "
            + positionClass.getName()
            + " has none");
  }
}
