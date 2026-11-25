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
package com.datasqrl.flinkrunner.connector.camel.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/** Extracts the type arguments a user class gives one of the source contracts. */
@Internal
public final class ContractTypes {

  private ContractTypes() {}

  /**
   * The type information of type argument {@code index} that {@code source} gives {@code contract}.
   *
   * @throws IllegalStateException if the argument is generic in {@code source}
   */
  public static <T> TypeInformation<T> argument(Class<?> contract, Object source, int index) {
    try {
      return TypeExtractor.createTypeInfo(contract, source.getClass(), index, null, null);
    } catch (RuntimeException e) {
      throw new IllegalStateException(
          "Cannot extract type argument "
              + index
              + " of "
              + contract.getSimpleName()
              + " from "
              + source.getClass().getName()
              + ", pass the type explicitly",
          e);
    }
  }
}
