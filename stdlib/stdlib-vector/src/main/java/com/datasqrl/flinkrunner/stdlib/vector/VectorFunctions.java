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
package com.datasqrl.flinkrunner.stdlib.vector;

import java.util.Set;
import org.apache.flink.table.functions.FunctionDefinition;

public class VectorFunctions {

  public static final cosine_similarity COSINE_SIMILARITY = new cosine_similarity();
  public static final cosine_distance COSINE_DISTANCE = new cosine_distance();

  public static final euclidean_distance EUCLIDEAN_DISTANCE = new euclidean_distance();

  public static final vector_to_double VECTOR_TO_DOUBLE = new vector_to_double();

  public static final double_to_vector DOUBLE_TO_VECTOR = new double_to_vector();

  public static final ascii_text_test_embed ASCII_TEXT_TEST_EMBED = new ascii_text_test_embed();

  public static final center CENTER = new center();

  public static final Set<FunctionDefinition> functions =
      Set.of(
          COSINE_SIMILARITY,
          COSINE_DISTANCE,
          EUCLIDEAN_DISTANCE,
          VECTOR_TO_DOUBLE,
          DOUBLE_TO_VECTOR,
          ASCII_TEXT_TEST_EMBED,
          CENTER);

  public static FlinkVectorType convert(double[] vector) {
    return new FlinkVectorType(vector);
  }
}
