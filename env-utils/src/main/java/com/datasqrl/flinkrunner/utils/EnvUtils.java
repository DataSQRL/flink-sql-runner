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
package com.datasqrl.flinkrunner.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/** Utility class for environment variable operations. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EnvUtils {
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
  public static Map<String, String> getEnvWithDeploymentDefaults() {
    return addDeploymentDefaults(System.getenv());
  }

  /**
   * Returns a copy of the supplied environment variables with deployment-specific defaults added.
   *
   * <p>Deployment defaults are only added when the supplied map does not already contain those
   * keys. Existing values are preserved.
   *
   * @param envVars environment variables to augment with deployment defaults
   * @return an immutable map containing the supplied variables plus any missing deployment defaults
   */
  public static Map<String, String> addDeploymentDefaults(Map<String, String> envVars) {
    var env = new HashMap<>(envVars);
    getDeploymentDefaults().forEach(env::putIfAbsent);

    return Map.copyOf(env);
  }

  public static Map<String, String> getDeploymentDefaults() {
    return Map.of(
        "DEPLOYMENT_ID",
        UUID.randomUUID().toString(),
        "DEPLOYMENT_TIMESTAMP",
        String.valueOf(System.currentTimeMillis()));
  }
}
