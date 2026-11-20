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
import com.datasqrl.flinkrunner.camel.CamelSource;
import com.datasqrl.flinkrunner.camel.Json;
import com.datasqrl.flinkrunner.camel.Types;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Instant;
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SQS orders in ack mode. */
@CamelSource
public class Orders implements AckSource<Orders.Order> {

  private static final Logger LOG = LoggerFactory.getLogger(Orders.class);

  public record Order(String orderId, String customerId, BigDecimal amount, Instant createdAt) {}

  public String endpoint() {
    return "aws2-sqs://orders-queue?region=us-west-2"
        + "&useDefaultCredentialsProvider=true"
        + "&deleteAfterRead=true"
        + "&visibilityTimeout=120&extendMessageVisibility=true"
        + "&waitTimeSeconds=20";
  }

  public Order map(Exchange e) {
    JsonNode j = Json.body(e);
    if (!j.hasNonNull("orderId")) {
      LOG.warn("Dropping message without orderId: {}", Types.truncate(j, 500));
      return null;
    }
    return new Order(
        j.get("orderId").asText(),
        j.path("customer").path("id").asText(null),
        Types.decimal(j.get("amount"), 18, 2),
        Types.instant(j.get("createdAt")));
  }
}
