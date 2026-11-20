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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.flinkrunner.camel.Request;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;
import sample.camel.RecentChanges.Change;

class RecentChangesTest {

  private final RecentChanges source = new RecentChanges();

  @Test
  void parsesChangesAndFollowsContinuation() {
    Exchange response =
        response(
            """
            {"continue": {"rccontinue": "20261029120001|42", "continue": "-||"},
             "query": {"recentchanges": [
               {"timestamp": "2026-10-29T12:00:00Z", "title": "Flink", "user": "alice"},
               {"timestamp": "2026-10-29T12:00:01Z", "title": "Camel", "user": "bob"}]}}
            """);

    List<Change> changes = new ArrayList<>();
    for (JsonNode record : source.records(response)) {
      changes.add(source.map(record));
    }
    assertEquals(
        List.of(
            new Change(Instant.parse("2026-10-29T12:00:00Z"), "Flink", "alice"),
            new Change(Instant.parse("2026-10-29T12:00:01Z"), "Camel", "bob")),
        changes);

    Request next = source.next(response).orElseThrow();
    String query = (String) next.headers().get(Exchange.HTTP_QUERY);
    assertEquals(true, query.contains("rccontinue=20261029120001%7C42"), query);
  }

  @Test
  void lastPageHasNoNext() {
    Exchange response = response("{\"query\": {\"recentchanges\": []}}");
    assertEquals(List.of(), iterate(source.records(response)));
    assertEquals(true, source.next(response).isEmpty());
  }

  @Test
  void requestStartsAtPosition() {
    String query =
        (String)
            source
                .request(Instant.parse("2026-10-29T12:00:00Z"))
                .headers()
                .get(Exchange.HTTP_QUERY);
    assertEquals(true, query.contains("rcstart=2026-10-29T12%3A00%3A00Z"), query);
    assertEquals(true, query.contains("rcdir=newer"), query);
  }

  @Test
  void initialHasWholeSeconds() {
    assertEquals(0, source.initial().getNano());
  }

  private static Exchange response(String json) {
    Exchange exchange = new DefaultExchange(new DefaultCamelContext());
    exchange.getMessage().setBody(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    return exchange;
  }

  private static List<JsonNode> iterate(Iterable<JsonNode> nodes) {
    List<JsonNode> list = new ArrayList<>();
    nodes.forEach(list::add);
    return list;
  }
}
