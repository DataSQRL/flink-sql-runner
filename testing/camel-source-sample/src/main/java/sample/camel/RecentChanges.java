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
package sample.camel;

import com.datasqrl.flinkrunner.camel.CamelSource;
import com.datasqrl.flinkrunner.camel.CursorSource;
import com.datasqrl.flinkrunner.camel.Json;
import com.datasqrl.flinkrunner.camel.Request;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.camel.Exchange;
import org.apache.camel.component.http.HttpClientConfigurer;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;

/**
 * The English Wikipedia's recent changes in cursor mode, through the MediaWiki recent changes API:
 * oldest first from {@code rcstart} (inclusive), paging with {@code rccontinue}. Starts a minute
 * before the job's first poll.
 */
@CamelSource
public class RecentChanges implements CursorSource<RecentChanges.Change, Instant, JsonNode> {

  static final String WIKIPEDIA = "https://en.wikipedia.org/w/api.php";
  static final Duration START_LOOKBACK = Duration.ofMinutes(1);
  // Wikimedia rejects requests without a descriptive User-Agent.
  static final String USER_AGENT = "flink-camel-connector-example/0.1";
  static final int PAGE_SIZE = 100;

  private final String endpoint;

  public RecentChanges() {
    this(WIKIPEDIA);
  }

  protected RecentChanges(String api) {
    this.endpoint = api + "?httpClientConfigurer=#class:" + NoAutomaticRetries.class.getName();
  }

  public record Change(Instant timestamp, String title, String user) {}

  /**
   * HttpClient retries a 429 itself after sleeping through its Retry-After, inside one Camel
   * request. A cancel waits for the fetcher only 30 seconds, so the wait belongs in the source's
   * {@code retry} backoff, which never blocks for longer than the fetch timeout.
   */
  public static final class NoAutomaticRetries implements HttpClientConfigurer {
    @Override
    public void configureHttpClient(HttpClientBuilder builder) {
      builder.disableAutomaticRetries();
    }
  }

  @Override
  public Instant initial() {
    // Change timestamps have whole seconds; a finer start would be ahead of its own records.
    return Instant.now().minus(START_LOOKBACK).truncatedTo(ChronoUnit.SECONDS);
  }

  @Override
  public Request request(Instant since) {
    return query(Map.of("rcstart", since.toString()));
  }

  @Override
  public Optional<Request> next(Exchange response) {
    return Optional.ofNullable(Json.body(response).path("continue").path("rccontinue").textValue())
        .map(token -> query(Map.of("rccontinue", token)));
  }

  @Override
  public Iterable<JsonNode> records(Exchange response) {
    return Json.body(response).path("query").path("recentchanges");
  }

  @Override
  public Change map(JsonNode change) {
    return new Change(
        Instant.parse(change.path("timestamp").asText()),
        change.path("title").asText(),
        change.path("user").asText());
  }

  @Override
  public Instant position(Change change) {
    return change.timestamp();
  }

  private Request query(Map<String, String> cursor) {
    Map<String, String> params =
        Map.of(
            "action", "query",
            "format", "json",
            "formatversion", "2",
            "list", "recentchanges",
            "rcdir", "newer",
            "rcprop", "title|timestamp|user",
            "rcshow", "!bot",
            "rclimit", String.valueOf(PAGE_SIZE));
    String query =
        Stream.concat(params.entrySet().stream(), cursor.entrySet().stream())
            .map(
                param ->
                    param.getKey()
                        + "="
                        + URLEncoder.encode(param.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));
    return Request.to(endpoint).header(Exchange.HTTP_QUERY, query).header("User-Agent", USER_AGENT);
  }
}
