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
package com.datasqrl.flinkrunner.stdlib.openai;

import static com.datasqrl.flinkrunner.stdlib.openai.OpenAiConstants.*;

import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class OpenAiEmbeddings {

  private static final int TOKEN_LIMIT = 8192;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final HttpClient httpClient;

  public OpenAiEmbeddings() {
    this(HttpClient.newHttpClient());
  }

  public OpenAiEmbeddings(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public FlinkVectorType vectorEmbed(String text, String modelName)
      throws IOException, InterruptedException {
    if (text == null || modelName == null) {
      return null;
    }

    return vectorEmbed(text, modelName, TOKEN_LIMIT);
  }

  public FlinkVectorType vectorEmbed(String text, String modelName, int tokenLimit)
      throws IOException, InterruptedException {
    // Truncate text to fit the maximum token limit
    text = truncateText(text, tokenLimit);

    // Create the request body JSON
    ObjectNode requestBody = objectMapper.createObjectNode();
    requestBody.put("input", text);
    requestBody.put("model", modelName);

    // Build the HTTP request
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    Optional.ofNullable(System.getenv(EMBEDDING_API_URL))
                        .orElse(DEFAULT_EMBEDDING_API)))
            .header("Authorization", "Bearer " + System.getenv(API_KEY))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(requestBody.toString(), StandardCharsets.UTF_8))
            .build();

    // Send the request and get the response
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Handle the response
    if (response.statusCode() == 200) {
      return parseEmbeddingVector(response.body());
    } else {
      throw new IOException(
          String.format("Failed to get embedding: HTTP status code %d", response.statusCode()));
    }
  }

  private static FlinkVectorType parseEmbeddingVector(String responseBody) throws IOException {
    // Parse JSON response
    JsonNode jsonResponse = objectMapper.readTree(responseBody);
    ArrayNode embeddingArray = (ArrayNode) jsonResponse.get("data").get(0).get("embedding");

    // Convert JSON array to a double array (embedding vector)
    double[] embeddingVector = new double[embeddingArray.size()];
    for (int i = 0; i < embeddingArray.size(); i++) {
      embeddingVector[i] = embeddingArray.get(i).asDouble();
    }
    return new FlinkVectorType(embeddingVector);
  }

  // Method to truncate the text if it exceeds the token limit (adjust as needed)
  private static String truncateText(String text, int maxTokens) {
    if (text.length() > maxTokens) {
      // Simple truncation based on character length (you can improve this to token-based
      // truncation)
      return text.substring(0, maxTokens);
    }
    return text;
  }
}
