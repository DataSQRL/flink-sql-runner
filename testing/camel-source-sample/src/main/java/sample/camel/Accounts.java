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
import com.datasqrl.flinkrunner.camel.Soql;
import com.datasqrl.flinkrunner.camel.Types;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.apache.camel.Exchange;

/** Salesforce accounts in cursor mode. */
@CamelSource
public class Accounts implements CursorSource<Accounts.Account, Instant, JsonNode> {

  public record Account(
      String id,
      String name,
      String industry,
      BigDecimal annualRevenue,
      Instant modifiedAt,
      boolean deleted) {}

  public Instant initial() {
    return Instant.EPOCH;
  }

  public Request request(Instant since) {
    return Request.to("salesforce:queryAll?rawPayload=true")
        .body(
            "SELECT Id, Name, Industry, AnnualRevenue, SystemModstamp, IsDeleted"
                + " FROM Account"
                + " WHERE SystemModstamp >= "
                + Soql.datetime(since) // whole seconds
                + " ORDER BY SystemModstamp, Id");
  }

  public Optional<Request> next(Exchange response) { // follow pages, no LIMIT
    JsonNode body = Json.body(response);
    if (body.path("done").asBoolean(true)) return Optional.empty();
    return Optional.of(
        Request.to("salesforce:queryMore?rawPayload=true")
            .body(body.get("nextRecordsUrl").asText()));
  }

  public Iterable<JsonNode> records(Exchange response) {
    return Json.body(response).get("records");
  }

  public Account map(JsonNode r) {
    return new Account(
        r.get("Id").asText(),
        r.get("Name").asText(),
        r.path("Industry").asText(null),
        Types.decimal(r.get("AnnualRevenue"), 18, 2),
        Types.instant(r.get("SystemModstamp")),
        r.get("IsDeleted").asBoolean());
  }

  public Instant position(Account a) {
    return a.modifiedAt().truncatedTo(ChronoUnit.SECONDS);
  }
}
