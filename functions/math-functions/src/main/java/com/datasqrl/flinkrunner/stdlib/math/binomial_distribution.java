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
package com.datasqrl.flinkrunner.stdlib.math;

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

/** Calculates the probability mass function for a binomial distribution. */
@AutoService(ScalarFunction.class)
public class binomial_distribution extends ScalarFunction {
  public Double eval(Long trials, Double probability, Long x) {
    if (trials == null || probability == null || x == null) return null;
    org.apache.commons.math3.distribution.BinomialDistribution dist =
        new org.apache.commons.math3.distribution.BinomialDistribution(
            trials.intValue(), probability);
    return dist.probability(x.intValue());
  }
}
