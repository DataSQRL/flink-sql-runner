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

import com.datasqrl.flinkrunner.camel.AckSource;
import com.datasqrl.flinkrunner.camel.Bind;
import com.datasqrl.flinkrunner.camel.CamelSource;
import com.datasqrl.flinkrunner.camel.Json;
import com.datasqrl.flinkrunner.camel.Types;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.jms.ConnectionFactory;
import java.time.Instant;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;

/** JMS shipments in ack mode. */
@CamelSource
public class Shipments implements AckSource<Shipments.Shipment> {

  public record Shipment(String shipmentId, String status, String carrier, Instant updatedAt) {}

  @Bind("cf")
  public ConnectionFactory cf() {
    return new ActiveMQConnectionFactory(System.getenv("BROKER_URL"));
  }

  public String endpoint() {
    return "jms:queue:shipments?connectionFactory=#cf";
  }

  public Shipment map(Exchange e) {
    JsonNode j = Json.body(e);
    if ("TEST".equals(j.path("env").asText())) return null;
    return new Shipment(
        j.get("id").asText(),
        j.get("status").asText().toUpperCase(),
        j.path("carrier").asText(null),
        Types.instant(j.get("ts")));
  }
}
