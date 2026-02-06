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
package com.datasqrl.flinkrunner.stdlib.math;

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

/** Calculates the cumulative probability for a Poisson distribution. */
@AutoService(ScalarFunction.class)
public class poisson_distribution extends ScalarFunction {
  public Double eval(Double mean, Long x) {
    if (mean == null || x == null) return null;
    org.apache.commons.math3.distribution.PoissonDistribution dist =
        new org.apache.commons.math3.distribution.PoissonDistribution(mean);
    return dist.cumulativeProbability(x.intValue());
  }
}
