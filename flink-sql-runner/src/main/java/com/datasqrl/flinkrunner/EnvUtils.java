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
package com.datasqrl.flinkrunner;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** Utility class for environment variable operations. */
final class EnvUtils {

  /**
   * Returns a map of environment variables with deployment-specific defaults.
   *
   * <p>This method creates a copy of the current system environment variables and adds default
   * values for the following variables if they are not already set:
   *
   * <ul>
   *   <li>{@code DEPLOYMENT_ID} - A unique identifier for the deployment (random UUID)
   *   <li>{@code DEPLOYMENT_TIMESTAMP} - The deployment timestamp in milliseconds since epoch
   * </ul>
   *
   * @return an immutable map containing all environment variables with defaults applied
   */
  static Map<String, String> getEnvWithDefaults() {
    var env = new HashMap<>(System.getenv());
    env.putIfAbsent("DEPLOYMENT_ID", UUID.randomUUID().toString());
    env.putIfAbsent("DEPLOYMENT_TIMESTAMP", String.valueOf(System.currentTimeMillis()));

    return Map.copyOf(env);
  }

  private EnvUtils() {
    throw new UnsupportedOperationException();
  }
}
