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
package com.datasqrl.flinkrunner.camel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import java.io.UncheckedIOException;
import org.apache.camel.Exchange;

/** Parses JSON text and message bodies into Jackson trees. */
public final class Json {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String PARSED_BODY_PROPERTY = "datasqrl.camel.json.body";

  private Json() {}

  public static JsonNode parse(String text) {
    try {
      return MAPPER.readTree(text);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * The exchange's message body as a JSON tree, or a missing node when the body is empty. The tree
   * is parsed on the first call and kept on the exchange, so several contract methods can read one
   * streamed body.
   */
  public static JsonNode body(Exchange exchange) {
    JsonNode parsed = exchange.getProperty(PARSED_BODY_PROPERTY, JsonNode.class);
    if (parsed == null) {
      String text = exchange.getMessage().getBody(String.class);
      parsed = text == null || text.isBlank() ? MissingNode.getInstance() : parse(text);
      exchange.setProperty(PARSED_BODY_PROPERTY, parsed);
    }
    return parsed;
  }
}
