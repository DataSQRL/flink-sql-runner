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

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

/** Environment variable resolving functionality. */
@Slf4j
public class EnvVarResolver {

  private static final Pattern ENVIRONMENT_VARIABLE_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

  private final Map<String, String> envVars;
  private final ObjectMapper objectMapper;

  public EnvVarResolver() {
    this(System.getenv());
  }

  public EnvVarResolver(Map<String, String> envVars) {
    this.envVars = envVars;
    objectMapper = initObjectMapper();
  }

  /**
   * Resolves environment variables referenced in a given source string. Searches for environment
   * variable references based on {@link EnvVarResolver#ENVIRONMENT_VARIABLE_PATTERN}. If a blank
   * source string is passed, it will be returned as is.
   *
   * @param src given source string that may contain environment variable references
   * @return a new string with the resolved environment variables
   * @throws IllegalStateException if any referenced environment variable are not available
   */
  public String resolve(String src) {
    if (StringUtils.isBlank(src)) {
      return src;
    }

    var res = new StringBuffer();
    // First pass to replace environment variables
    var matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(src);
    var missingEnvVars = new HashSet<String>();
    while (matcher.find()) {
      var rawKey = matcher.group(1);
      String key;
      String defaultValue = null;

      // Split at first ':' to support default values
      int colonIdx = rawKey.indexOf(':');
      if (colonIdx >= 0) {
        key = rawKey.substring(0, colonIdx);
        defaultValue = rawKey.substring(colonIdx + 1);
      } else {
        key = rawKey;
      }

      if (envVars.containsKey(key)) {
        var envValue = envVars.get(key);
        matcher.appendReplacement(res, Matcher.quoteReplacement(envValue));
      } else if (defaultValue != null) {
        matcher.appendReplacement(res, Matcher.quoteReplacement(defaultValue));
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
   * EnvVarResolver#ENVIRONMENT_VARIABLE_PATTERN}.
   *
   * @param jsonSrc given JSON source string that may contain environment variable references
   * @return JSON string with the resolved environment variables
   * @throws IOException if the JSON processing fails in any way
   */
  public String resolveInJson(String jsonSrc) throws IOException {
    var res = objectMapper.readValue(jsonSrc, Map.class);

    return objectMapper.writeValueAsString(res);
  }

  private ObjectMapper initObjectMapper() {
    var objectMapper = new ObjectMapper();

    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);

    return objectMapper;
  }

  private class JsonEnvVarDeserializer extends JsonDeserializer<String> {

    @Override
    public String deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
      var value = parser.getText();
      return resolve(value);
    }
  }
}
