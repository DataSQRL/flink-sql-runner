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
package com.datasqrl.flinkrunner.connector.camel.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Runs the consumer against an embedded broker, which holds a message until it is committed. */
class TransactedJmsConsumerTest {

  private static final String QUEUE = "shipments";
  private static final long RECEIVE_TIMEOUT_MS = 2_000;

  private TestJmsBroker broker;
  private DefaultCamelContext camel;
  private TransactedJmsConsumer consumer;

  @BeforeEach
  void start() throws Exception {
    broker = new TestJmsBroker();
    camel = new DefaultCamelContext();
    camel.start();
    consumer = new TransactedJmsConsumer(camel.getEndpoint(broker.endpointUri("jms", QUEUE)));
  }

  @AfterEach
  void stop() throws Exception {
    consumer.close();
    camel.close();
    broker.close();
  }

  @Test
  void commitsOnceEveryReceivedMessageIsAcknowledged() throws Exception {
    broker.send(QUEUE, "a", "b", "c");
    assertEquals(List.of("a", "b", "c"), receive(3));

    consumer.acknowledged(3);

    assertEquals(0, broker.depth(QUEUE));
  }

  @Test
  void commitsNothingWhileAReceivedMessageIsUnacknowledged() throws Exception {
    broker.send(QUEUE, "a", "b", "c");
    receive(3);

    // The third message is still on its way to a checkpoint, and a commit would take it too.
    consumer.acknowledged(2);
    consumer.close();

    assertEquals(3, broker.depth(QUEUE));
  }

  @Test
  void pausesFromACheckpointUntilTheCommit() throws Exception {
    broker.send(QUEUE, "a");
    receive(1);
    assertFalse(consumer.paused());

    consumer.checkpointStarted();
    assertTrue(consumer.paused());

    consumer.acknowledged(1);
    assertFalse(consumer.paused());
  }

  @Test
  void keepsReceivingThroughACheckpointWithNothingToCommit() {
    consumer.checkpointStarted();

    assertFalse(consumer.paused());
  }

  @Test
  void reconnectsAfterAFailedReceive() throws Exception {
    broker.send(QUEUE, "a");
    receive(1);
    int port = broker.openWirePort();
    broker.close();

    broker = new TestJmsBroker(port);
    broker.send(QUEUE, "b");

    assertEquals("b", awaitBody());
  }

  @Test
  void survivesAFailedCommit() throws Exception {
    broker.send(QUEUE, "a");
    receive(1);
    broker.close();

    // Throwing here would take down the thread that acknowledges, and the task manager with it.
    consumer.acknowledged(1);

    assertFalse(consumer.paused());
  }

  @Test
  void rejectsTopics() throws Exception {
    String topicUri = broker.endpointUri("jms", QUEUE).replace(":queue:", ":topic:");

    assertThrows(
        IllegalArgumentException.class,
        () -> new TransactedJmsConsumer(camel.getEndpoint(topicUri)));
  }

  private List<String> receive(int count) {
    List<String> bodies = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      bodies.add(consumer.receive(RECEIVE_TIMEOUT_MS).getMessage().getBody(String.class));
    }
    return bodies;
  }

  /** The next message, receiving again for as long as the consumer finds its connection dead. */
  private String awaitBody() {
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (System.nanoTime() < deadline) {
      try {
        Exchange exchange = consumer.receive(RECEIVE_TIMEOUT_MS);
        if (exchange != null) {
          return exchange.getMessage().getBody(String.class);
        }
      } catch (IllegalStateException connectionLost) {
        // The next receive connects again.
      }
    }
    return fail("nothing received after the broker came back");
  }
}
