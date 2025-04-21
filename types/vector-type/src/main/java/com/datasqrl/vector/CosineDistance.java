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

import com.datasqrl.types.vector.FlinkVectorType;

/** Computes the cosine distance between two vectors */
public class CosineDistance extends CosineSimilarity {

  public double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    return 1 - super.eval(vectorA, vectorB);
  }
}
