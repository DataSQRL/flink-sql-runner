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
package com.datasqrl.flinkrunner.functions.openai;

import static com.datasqrl.flinkrunner.functions.openai.OpenAIUtil.API_KEY;
import static com.datasqrl.flinkrunner.functions.openai.OpenAIUtil.COMPLETIONS_API;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class OpenAICompletions {

  private static final double TEMPERATURE_DEFAULT = 1.0;
  private static final double TOP_P_DEFAULT = 1.0;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final HttpClient httpClient;

  public OpenAICompletions() {
    this(HttpClient.newHttpClient());
  }

  public OpenAICompletions(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public String callCompletions(CompletionsRequest request)
      throws IOException, InterruptedException {
    if (request.getPrompt() == null || request.getModelName() == null) {
      return null;
    }

    // Create the request body JSON
    final ObjectNode requestBody = createRequestBody(request);

    // Build the HTTP request
    final HttpRequest httpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create(COMPLETIONS_API))
            .header("Authorization", "Bearer " + System.getenv(API_KEY))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(requestBody.toString(), StandardCharsets.UTF_8))
            .build();

    // Send the request and get the response
    final HttpResponse<String> response =
        httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

    // Handle the response
    if (response.statusCode() == 200) {
      return extractContent(response.body());
    } else {
      throw new IOException(
          String.format(
              "Failed to get completion: HTTP status code %d Message: %s",
              response.statusCode(), response.body()));
    }
  }

  private ObjectNode createRequestBody(CompletionsRequest request) {
    final ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("model", request.getModelName());

    // Create the messages array as required by the chat completions endpoint
    final ArrayNode messagesArray = objectMapper.createArrayNode();

    if (Boolean.TRUE.equals(request.isRequireJsonOutput())) {
      if (request.getJsonSchema() != null) {
        JsonNode schemaNode;
        try {
          schemaNode = objectMapper.readTree(request.getJsonSchema());
        } catch (JsonProcessingException e) {
          throw new RuntimeException("Failed to parse JSON schema", e);
        }

        // `json_schema.name` is a mandatory field, but unsure for what the value is used.
        // It could be used for caching, but after a few tests, it seems to be ignored.
        requestBody
            .putObject("response_format")
            .put("type", "json_schema")
            .putObject("json_schema")
            .put("name", "extract_json_schema_name")
            .put("strict", true)
            .set("schema", schemaNode);
      } else {
        requestBody.putObject("response_format").put("type", "json_object");
      }

      messagesArray.add(
          createMessage("system", "You are a helpful assistant designed to output minified JSON."));
    }

    messagesArray.add(createMessage("user", request.getPrompt()));

    requestBody.set("messages", messagesArray);
    requestBody.put(
        "temperature",
        request.getTemperature() == null ? TEMPERATURE_DEFAULT : request.getTemperature());
    requestBody.put("top_p", request.getTopP() == null ? TOP_P_DEFAULT : request.getTopP());
    requestBody.put("n", 1); // Number of completions to generate

    if (request.getMaxOutputTokens() != null) {
      requestBody.put("max_tokens", request.getMaxOutputTokens());
    }

    return requestBody;
  }

  private String extractContent(String jsonResponse) throws IOException {
    JsonNode jsonNode = objectMapper.readTree(jsonResponse);
    // Extract the content from the first choice
    return jsonNode.get("choices").get(0).get("message").get("content").asText().trim();
  }

  private static ObjectNode createMessage(String role, String prompt) {
    ObjectNode userMessage = objectMapper.createObjectNode();
    userMessage.put("role", role);
    userMessage.put("content", prompt);
    return userMessage;
  }
}
