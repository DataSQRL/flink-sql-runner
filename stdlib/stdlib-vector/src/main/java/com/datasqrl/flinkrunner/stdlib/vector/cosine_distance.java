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
package com.datasqrl.flinkrunner.stdlib.vector;

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/** Computes the cosine distance between two vectors */
@AutoService(AutoRegisterSystemFunction.class)
public class cosine_distance extends cosine_similarity {

  private static final long serialVersionUID = -7136217222147264375L;

  @Override
  public Double eval(FlinkVectorType vectorA, FlinkVectorType vectorB) {
    var similarity = super.eval(vectorA, vectorB);
    if (similarity == null) {
      return null;
    }

    return 1 - similarity;
  }
}
