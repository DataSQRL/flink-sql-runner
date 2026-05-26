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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

/**
 * Environment variable resolving functionality.
 *
 * <p>Resolver instances replace {@code ${VAR}} placeholders with values from a configured
 * environment map. Missing variables either fail resolution in strict mode or remain unresolved in
 * non-strict mode.
 */
@Slf4j
public class EnvVarResolver {

  private static final Pattern ENVIRONMENT_VARIABLE_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

  private final Map<String, String> envVars;
  private final ObjectMapper objectMapper;
  private final boolean strict;

  private EnvVarResolver(Map<String, String> envVars, boolean strict) {
    this.envVars = envVars;
    this.strict = strict;
    objectMapper = initObjectMapper();
  }

  /**
   * Creates a strict resolver backed by the current process environment.
   *
   * @return a resolver backed by {@link System#getenv()}
   */
  public static EnvVarResolver of() {
    return of(true);
  }

  /**
   * Creates a resolver backed by the current process environment.
   *
   * @param strict whether missing variables should fail resolution
   * @return a resolver backed by {@link System#getenv()}
   */
  public static EnvVarResolver of(boolean strict) {
    return of(System.getenv(), strict);
  }

  /**
   * Creates a strict resolver backed by the supplied environment variables.
   *
   * @param envVars environment variables used for placeholder resolution
   * @return a resolver backed by the supplied environment variables
   */
  public static EnvVarResolver of(Map<String, String> envVars) {
    return of(envVars, true);
  }

  /**
   * Creates a resolver backed by the supplied environment variables.
   *
   * @param envVars environment variables used for placeholder resolution
   * @param strict whether missing variables should fail resolution
   * @return a resolver backed by the supplied environment variables
   */
  public static EnvVarResolver of(Map<String, String> envVars, boolean strict) {
    return new EnvVarResolver(envVars, strict);
  }

  /**
   * Creates a strict resolver backed by the current process environment plus deployment defaults.
   *
   * @return a resolver with deployment defaults applied
   */
  public static EnvVarResolver withDeploymentDefaults() {
    return withDeploymentDefaults(true);
  }

  /**
   * Creates a resolver backed by the current process environment plus deployment defaults.
   *
   * @param strict whether missing variables should fail resolution
   * @return a resolver with deployment defaults applied
   */
  public static EnvVarResolver withDeploymentDefaults(boolean strict) {
    return new EnvVarResolver(EnvUtils.getEnvWithDeploymentDefaults(), strict);
  }

  /**
   * Creates a strict resolver backed by the supplied environment variables plus deployment
   * defaults.
   *
   * @param envVars environment variables used for placeholder resolution
   * @return a resolver with deployment defaults applied
   */
  public static EnvVarResolver withDeploymentDefaults(Map<String, String> envVars) {
    return withDeploymentDefaults(envVars, true);
  }

  /**
   * Creates a resolver backed by the supplied environment variables plus deployment defaults.
   *
   * <p>Deployment defaults are only added when the supplied map does not already contain those
   * keys.
   *
   * @param envVars environment variables used for placeholder resolution
   * @param strict whether missing variables should fail resolution
   * @return a resolver with deployment defaults applied
   */
  public static EnvVarResolver withDeploymentDefaults(Map<String, String> envVars, boolean strict) {
    var modifiedEnvVars = new HashMap<>(envVars);
    EnvUtils.getDeploymentDefaults().forEach(modifiedEnvVars::putIfAbsent);

    return new EnvVarResolver(Map.copyOf(modifiedEnvVars), strict);
  }

  /**
   * Resolves environment variables referenced in a given source string. Searches for environment
   * variable references based on {@link EnvVarResolver#ENVIRONMENT_VARIABLE_PATTERN}. If a blank
   * source string is passed, it will be returned as is.
   *
   * @param src given source string that may contain environment variable references
   * @return a new string with the resolved environment variables
   * @throws IllegalStateException if strict mode is enabled and any referenced environment variable
   *     is not available
   */
  public String resolve(String src) {
    if (src == null || src.isBlank()) {
      return src;
    }

    var res = new StringBuilder();
    // First pass to replace environment variables
    var matcher = ENVIRONMENT_VARIABLE_PATTERN.matcher(src);
    var missingEnvVars = new HashSet<String>();
    while (matcher.find()) {
      var rawKey = matcher.group(1);
      String key;
      String defaultValue = null;

      // Support bash-style default values: ${VAR:-default} or ${VAR:=default}
      int splitIdx = rawKey.indexOf(":-");
      if (splitIdx == -1) {
        splitIdx = rawKey.indexOf(":=");
      }

      if (splitIdx >= 0) {
        key = rawKey.substring(0, splitIdx);
        defaultValue = rawKey.substring(splitIdx + 2);
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

    if (strict && !missingEnvVars.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "The following environment variables were referenced, but not found: %s",
              missingEnvVars));
    }

    return res.toString();
  }

  /**
   * Resolves environment variables referenced in a given JSON source string. Searches for
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

  /**
   * Creates an {@link ObjectMapper} configured to resolve environment variables in string values.
   *
   * @return an object mapper with environment-variable resolution enabled for string
   *     deserialization
   */
  public ObjectMapper initObjectMapper() {
    return initObjectMapper(new ObjectMapper());
  }

  /**
   * Configures an {@link ObjectMapper} to resolve environment variables in string values.
   *
   * @param mapper mapper to configure
   * @return the supplied mapper with environment-variable resolution enabled for string
   *     deserialization
   */
  public ObjectMapper initObjectMapper(ObjectMapper mapper) {
    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    mapper.registerModule(module);

    return mapper;
  }

  private class JsonEnvVarDeserializer extends JsonDeserializer<String> {

    @Override
    public String deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
      var value = parser.getText();
      return resolve(value);
    }
  }
}
