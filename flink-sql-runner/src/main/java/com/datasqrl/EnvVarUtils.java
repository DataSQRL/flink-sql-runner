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
package com.datasqrl;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

/** Environment variable handling utilities. */
@Slf4j
class EnvVarUtils {

  private static final Pattern ENVIRONMENT_VARIABLE_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");
  private static final ObjectMapper OBJECT_MAPPER = initObjectMapper();

  /** See {@link EnvVarUtils#resolveEnvVars(String, Map)}. */
  static String resolveEnvVars(String src) {
    return resolveEnvVars(src, System.getenv());
  }

  /**
   * Resolves environment variables referenced in a given source string. Searches for environment
   * variable references based on {@link EnvVarUtils#ENVIRONMENT_VARIABLE_PATTERN}. If a blank
   * source string is passed, it will be returned as is.
   *
   * @param src given source string that may contain environment variable references
   * @param envVars available environment variables
   * @return a new string with the resolved environment variables
   * @throws IllegalStateException if any referenced environment variable are not available
   */
  static String resolveEnvVars(String src, Map<String, String> envVars) {
    if (StringUtils.isBlank(src)) {
      return src;
    }

    var res = new StringBuffer();
    // First pass to replace environment variables
    var matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(src);
    var missingEnvVars = new HashSet<String>();
    while (matcher.find()) {
      var key = matcher.group(1);
      if (envVars.containsKey(key)) {
        var envValue = envVars.get(key);
        matcher.appendReplacement(res, Matcher.quoteReplacement(envValue));
      } else {
        missingEnvVars.add(key);
      }
    }
    matcher.appendTail(res);

    if (!missingEnvVars.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "The following environment variables were referenced, but not found: %s",
              missingEnvVars));
    }

    return res.toString();
  }

  /**
   * Resolves environment variables referenced in a given JSON source string.Searches for
   * environment variable references in any string leaf nodes based on {@link
   * EnvVarUtils#ENVIRONMENT_VARIABLE_PATTERN}.
   *
   * @param jsonSrc given JSON source string that may contain environment variable references
   * @return JSON string with the resolved environment variables
   * @throws IOException if the JSON processing fails in any way
   */
  static String resolveEnvVarsInJson(String jsonSrc) throws IOException {
    var res = OBJECT_MAPPER.readValue(jsonSrc, Map.class);

    return OBJECT_MAPPER.writeValueAsString(res);
  }

  private static ObjectMapper initObjectMapper() {
    var objectMapper = new ObjectMapper();

    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);

    return objectMapper;
  }

  private EnvVarUtils() {
    throw new UnsupportedOperationException();
  }
}
