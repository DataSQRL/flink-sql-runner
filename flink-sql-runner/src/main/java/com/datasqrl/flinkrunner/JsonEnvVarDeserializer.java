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
import java.util.Map;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;

class JsonEnvVarDeserializer extends JsonDeserializer<String> {

  private final Map<String, String> envVars;

  JsonEnvVarDeserializer() {
    this(System.getenv());
  }

  JsonEnvVarDeserializer(Map<String, String> envVars) {
    this.envVars = envVars;
  }

  @Override
  public String deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
    var value = parser.getText();
    return EnvVarUtils.resolveEnvVars(value, envVars);
  }
}
