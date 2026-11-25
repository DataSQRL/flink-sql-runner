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
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class PendingAcksTest {

  private final CamelContext camel = new DefaultCamelContext();

  @Test
  void acknowledgesOnlyWhatCompletedCheckpointsCover() {
    PendingAcks acks = new PendingAcks();
    Exchange first = new DefaultExchange(camel);
    Exchange second = new DefaultExchange(camel);
    Exchange third = new DefaultExchange(camel);

    acks.add(first);
    acks.snapshot(1);
    acks.add(second);
    acks.snapshot(2);
    acks.add(third);

    // Checkpoint 1 was aborted; completing 2 covers it too. The third exchange was emitted
    // after the last snapshot and must wait.
    assertEquals(List.of(first, second), acks.complete(2));
    assertEquals(List.of(), acks.complete(2));

    acks.snapshot(3);
    assertEquals(List.of(third), acks.complete(3));
  }
}
