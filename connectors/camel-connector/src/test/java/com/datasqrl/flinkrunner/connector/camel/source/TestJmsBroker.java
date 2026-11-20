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

import jakarta.jms.Connection;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQQueue;

/** An embedded ActiveMQ broker for tests, speaking OpenWire and AMQP on local ports. */
public final class TestJmsBroker implements AutoCloseable {

  private static final int ANY_PORT = 0;

  private final BrokerService broker = new BrokerService();
  private final TransportConnector openWire;
  private final TransportConnector amqp;

  public TestJmsBroker() throws Exception {
    this(ANY_PORT);
  }

  /** Listens for OpenWire on the given port, to come back where a stopped broker was. */
  public TestJmsBroker(int openWirePort) throws Exception {
    broker.setPersistent(false);
    broker.setUseJmx(false);
    openWire = broker.addConnector("tcp://localhost:" + openWirePort);
    amqp = broker.addConnector("amqp://localhost:" + ANY_PORT);
    broker.start();
  }

  public int openWirePort() throws Exception {
    return openWire.getConnectUri().getPort();
  }

  /**
   * A consumer endpoint for the queue, read through the {@code jms}, {@code activemq6} or {@code
   * amqp} component.
   */
  public String endpointUri(String component, String queue) throws Exception {
    String connectionFactory =
        switch (component) {
          case "jms", "activemq6" ->
              "org.apache.activemq.ActiveMQConnectionFactory('"
                  + openWire.getPublishableConnectString()
                  + "')";
          case "amqp" ->
              "org.apache.qpid.jms.JmsConnectionFactory('amqp://localhost:"
                  + amqp.getConnectUri().getPort()
                  + "')";
          default -> throw new IllegalArgumentException(component);
        };
    return component + ":queue:" + queue + "?connectionFactory=#class:" + connectionFactory;
  }

  public void send(String queue, String... bodies) throws Exception {
    try (Connection connection = connect();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue(queue))) {
      for (String body : bodies) {
        producer.send(session.createTextMessage(body));
      }
    }
  }

  /** How many messages the broker still holds for the queue, delivered but uncommitted or not. */
  public long depth(String queue) throws Exception {
    return broker
        .getDestination(new ActiveMQQueue(queue))
        .getDestinationStatistics()
        .getMessages()
        .getCount();
  }

  private Connection connect() throws Exception {
    return new ActiveMQConnectionFactory(openWire.getPublishableConnectString()).createConnection();
  }

  @Override
  public void close() throws Exception {
    broker.stop();
  }
}
