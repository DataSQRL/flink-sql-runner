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
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.component.jms.JmsEndpoint;
import org.apache.camel.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a JMS queue through a transacted session the source owns. camel-jms's own polling consumer
 * commits or acknowledges a message inside {@code receive}, before any checkpoint can cover it.
 *
 * <p>A JMS transaction commits everything the session received, so the commit waits until every
 * received message is acknowledged, that is, was emitted before a checkpoint that completed. So
 * that a steady flow of messages can't put that off forever, receiving pauses from a checkpoint's
 * snapshot until the commit. A checkpoint commits when it completes if every received message was
 * emitted before its snapshot; otherwise the commit takes one more checkpoint.
 *
 * <p>The endpoint supplies the connection factory, the queue, the selector and the mapping of JMS
 * messages to exchanges. camel-jms's other consumer options don't apply.
 *
 * <p>The fetcher thread receives and the acker thread commits, one at a time: a JMS session must
 * not be used by two threads at once.
 */
final class TransactedJmsConsumer implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(TransactedJmsConsumer.class);

  private final JmsEndpoint endpoint;
  private final String sanitizedUri;
  // Fair, so that a commit is not starved by receives that each hold the lock for their timeout.
  // A commit still waits for the receive under way, so for up to the receive timeout.
  private final ReentrantLock sessionLock = new ReentrantLock(true);
  private final AtomicInteger unacknowledged = new AtomicInteger();
  private volatile boolean pauseRequested;

  private Connection connection;
  private Session session;
  private MessageConsumer consumer;

  TransactedJmsConsumer(Endpoint endpoint) {
    this.endpoint = (JmsEndpoint) endpoint;
    this.sanitizedUri = URISupport.sanitizeUri(endpoint.getEndpointUri());
    // A topic redelivers only to a durable subscription, which needs a subscription name and
    // a client id. Add them here when a source needs to read a topic.
    if (this.endpoint.isPubSubDomain()) {
      throw new IllegalArgumentException("Ack mode reads JMS queues, not topics: " + sanitizedUri);
    }
  }

  /**
   * Receives one message, waiting up to {@code timeoutMs}, or only taking one that is ready when
   * {@code timeoutMs} is 0. Called on the fetcher thread.
   */
  Exchange receive(long timeoutMs) {
    sessionLock.lock();
    try {
      connect();
      Message message = timeoutMs > 0 ? consumer.receive(timeoutMs) : consumer.receiveNoWait();
      if (message == null) {
        return null;
      }
      Exchange exchange = endpoint.createExchange(message, session);
      unacknowledged.incrementAndGet();
      return exchange;
    } catch (JMSException e) {
      // Closing rolls the transaction back, and the next receive connects again.
      disconnect();
      throw new IllegalStateException("Cannot receive from " + sanitizedUri, e);
    } finally {
      sessionLock.unlock();
    }
  }

  /** Called on the mailbox thread when a checkpoint takes its snapshot. Never blocks. */
  void checkpointStarted() {
    pauseRequested = true;
  }

  /**
   * Whether the fetcher should hold off receiving, so that the messages received so far can be
   * acknowledged and committed. A message that arrives in a receive already under way only puts the
   * commit off to the next checkpoint.
   */
  boolean paused() {
    if (pauseRequested && unacknowledged.get() == 0) {
      pauseRequested = false;
    }
    return pauseRequested;
  }

  /**
   * Counts exchanges that a completed checkpoint covers, and commits once every received message is
   * among them. Called on the acker thread, where an exception would take the task manager down.
   */
  void acknowledged(int exchanges) {
    sessionLock.lock();
    try {
      if (unacknowledged.addAndGet(-exchanges) == 0 && session != null) {
        session.commit();
      }
    } catch (JMSException | RuntimeException e) {
      LOG.warn(
          "Failed to commit the messages received from {}, the broker delivers them again",
          sanitizedUri,
          e);
      disconnect();
    } finally {
      sessionLock.unlock();
    }
  }

  private void connect() throws JMSException {
    if (consumer != null) {
      return;
    }
    connection =
        endpoint.getConfiguration().getOrCreateListenerConnectionFactory().createConnection();
    session = connection.createSession(true, Session.SESSION_TRANSACTED);
    consumer =
        session.createConsumer(
            session.createQueue(endpoint.getDestinationName()), endpoint.getSelector());
    connection.start();
  }

  private void disconnect() {
    if (connection != null) {
      try {
        connection.close();
      } catch (JMSException e) {
        LOG.debug("Failed to close the connection to {}", sanitizedUri, e);
      }
    }
    connection = null;
    session = null;
    consumer = null;
  }

  /** Rolls back whatever was never committed, so the broker delivers it again. */
  @Override
  public void close() {
    sessionLock.lock();
    try {
      disconnect();
    } finally {
      sessionLock.unlock();
    }
  }
}
