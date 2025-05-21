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
package com.datasqrl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class EnvironmentVariablesUtils {

  private static final Pattern ENVIRONMENT_VARIABLE_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

  public static String replaceWithEnv(String command) {
    return replaceWithEnv(command, System.getenv());
  }

  public static String replaceWithEnv(String command, Map<String, String> envVariables) {
    var substitutedStr = command;
    var result = new StringBuffer();
    // First pass to replace environment variables
    var matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(substitutedStr);
    while (matcher.find()) {
      var key = matcher.group(1);
      var envValue = envVariables.get(key);
      if (envValue == null) {
        throw new IllegalStateException(String.format("Missing environment variable: %s", key));
      }
      matcher.appendReplacement(result, Matcher.quoteReplacement(envValue));
    }
    matcher.appendTail(result);

    return result.toString();
  }

  public Set<String> validateEnvironmentVariables(String script) {
    return validateEnvironmentVariables(System.getenv(), script);
  }

  public Set<String> validateEnvironmentVariables(Map<String, String> envVariables, String script) {
    var matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(script);

    Set<String> scriptEnvironmentVariables = new TreeSet<>();
    while (matcher.find()) {
      scriptEnvironmentVariables.add(matcher.group(1));
    }

    if (envVariables.keySet().containsAll(scriptEnvironmentVariables)) {
      log.info("All environment variables are available: {}", scriptEnvironmentVariables);
      return Collections.emptySet();
    }

    scriptEnvironmentVariables.removeAll(envVariables.keySet());
    return Collections.unmodifiableSet(scriptEnvironmentVariables);
  }
}
