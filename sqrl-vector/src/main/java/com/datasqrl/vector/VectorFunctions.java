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
package com.datasqrl.vector;

import java.util.Set;
import org.apache.flink.table.functions.FunctionDefinition;

public class VectorFunctions {

  public static final CosineSimilarity COSINE_SIMILARITY = new CosineSimilarity();
  public static final CosineDistance COSINE_DISTANCE = new CosineDistance();

  public static final EuclideanDistance EUCLIDEAN_DISTANCE = new EuclideanDistance();

  public static final VectorToDouble VEC_TO_DOUBLE = new VectorToDouble();

  public static final DoubleToVector DOUBLE_TO_VECTOR = new DoubleToVector();

  public static final AsciiTextTestEmbed ASCII_TEXT_TEST_EMBED = new AsciiTextTestEmbed();

  public static final Center CENTER = new Center();

  public static final Set<FunctionDefinition> functions =
      Set.of(
          COSINE_SIMILARITY,
          COSINE_DISTANCE,
          EUCLIDEAN_DISTANCE,
          VEC_TO_DOUBLE,
          DOUBLE_TO_VECTOR,
          ASCII_TEXT_TEST_EMBED,
          CENTER);

  public static FlinkVectorType convert(double[] vector) {
    return new FlinkVectorType(vector);
  }
}
