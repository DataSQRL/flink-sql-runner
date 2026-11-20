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

import com.datasqrl.flinkrunner.camel.Bind;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.ConsumerTemplate;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.PropertyConfigurer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.PropertyBindingSupport;
import org.apache.camel.support.ScheduledPollEndpoint;
import org.apache.camel.util.URISupport;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Camel side of one source reader: a CamelContext, its ConsumerTemplate and ProducerTemplate,
 * and a thread that acknowledges exchanges.
 *
 * <p>No Camel call runs on the task's mailbox thread. The fetcher thread receives and sends, and
 * the acker thread runs {@link ConsumerTemplate#doneUoW(Exchange)}, which fires the component's own
 * commit (delete a file, delete an SQS message, ...). The runtime lives as long as the reader, not
 * the fetcher, so the acks of the final checkpoint still fire after a bounded split has finished.
 *
 * <p>camel-jms and the components built on it (amqp, activemq, ...) commit inside their own {@code
 * receive}, so their endpoints are read through a {@link TransactedJmsConsumer} instead, which the
 * acker thread commits.
 */
@Internal
public final class CamelRuntime implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(CamelRuntime.class);

  private static final long NO_WAIT = 0;
  private static final Duration PENDING_ACKS_TIMEOUT = Duration.ofSeconds(30);
  private static final char COMPONENT_SEPARATOR = '.';
  private static final String JMS_ENDPOINT_CLASS = "org.apache.camel.component.jms.JmsEndpoint";

  private final ClassLoader userClassLoader;
  private final Object source;
  private final Map<String, String> componentOptions;
  private final String endpointUri;
  private final PollErrors pollErrors;
  private final EndpointSetup endpointSetup;
  private final ExecutorService acker =
      Executors.newSingleThreadExecutor(new ExecutorThreadFactory("camel-source-acker"));
  private final AtomicInteger unacked = new AtomicInteger();

  private CamelContext context;
  private volatile ConsumerTemplate consumer;
  private volatile TransactedJmsConsumer jmsConsumer;
  private ProducerTemplate producer;

  /**
   * @param userClassLoader the job's classloader, which Camel discovers components through
   * @param source the user's source, whose {@link Bind} methods are bound in Camel's registry
   * @param componentOptions Camel component options, keyed {@code <component>.<option>}
   * @param endpointUri the endpoint the source receives from, or {@code null} when it only sends
   * @param pollErrors fetch errors, or {@code null} when they fail the fetch straight away
   * @param endpointSetup configures the endpoint before its consumer is created
   */
  public CamelRuntime(
      ClassLoader userClassLoader,
      Object source,
      Map<String, String> componentOptions,
      String endpointUri,
      PollErrors pollErrors,
      EndpointSetup endpointSetup) {
    this.userClassLoader = userClassLoader;
    this.source = source;
    this.componentOptions = componentOptions;
    this.endpointUri = endpointUri;
    this.pollErrors = pollErrors;
    this.endpointSetup = endpointSetup;
  }

  /** Configures the source's endpoint once, before Camel creates a consumer for it. */
  @FunctionalInterface
  public interface EndpointSetup {
    void configure(Endpoint endpoint) throws Exception;
  }

  /** Receives one exchange, waiting up to {@code timeoutMs}. Called on the fetcher thread. */
  public Exchange receive(String endpointUri, long timeoutMs) {
    ConsumerTemplate template = consumer();
    Exchange exchange =
        jmsConsumer != null
            ? jmsConsumer.receive(timeoutMs)
            : template.receive(endpointUri, timeoutMs);
    if (exchange != null) {
      unacked.incrementAndGet();
    }
    return exchange;
  }

  /** Receives one exchange if one is ready, without waiting. Called on the fetcher thread. */
  public Exchange receiveNoWait(String endpointUri) {
    ConsumerTemplate template = consumer();
    Exchange exchange =
        jmsConsumer != null ? jmsConsumer.receive(NO_WAIT) : template.receiveNoWait(endpointUri);
    if (exchange != null) {
      unacked.incrementAndGet();
    }
    return exchange;
  }

  /** Exchanges received and not yet acknowledged. */
  public int unacked() {
    return unacked.get();
  }

  /** Called on the mailbox thread when a checkpoint takes its snapshot. Never blocks. */
  public void checkpointStarted() {
    TransactedJmsConsumer jms = jmsConsumer;
    if (jms != null) {
      jms.checkpointStarted();
    }
  }

  /**
   * Whether the fetcher should hold off receiving until the exchanges received so far are
   * acknowledged. Only a {@link TransactedJmsConsumer} needs that, to commit them.
   */
  public boolean receivePaused() {
    TransactedJmsConsumer jms = jmsConsumer;
    return jms != null && jms.paused();
  }

  /**
   * Receives one exchange from the source's endpoint, waiting up to {@code timeoutMs}. With retries
   * on, an exception from {@code receive} counts as a failed attempt, and this throws once too many
   * attempts in a row have failed, whether they threw here or failed inside Camel's own polling.
   * Called on the fetcher thread.
   */
  public Exchange receiveWithRetries(long timeoutMs) throws IOException {
    if (pollErrors == null) {
      return receive(endpointUri, timeoutMs);
    }
    pollErrors.checkAttempts();
    if (!pollErrors.attemptAllowed()) {
      sleepQuietly(timeoutMs);
      return null;
    }
    try {
      Exchange exchange = receive(endpointUri, timeoutMs);
      if (exchange != null) {
        pollErrors.succeeded();
      }
      return exchange;
    } catch (RuntimeException e) {
      pollErrors.receiveFailed(e);
      pollErrors.checkAttempts();
      return null;
    }
  }

  /** Completes a received exchange right away, on the calling thread. */
  public void done(Exchange exchange) {
    try {
      consumer.doneUoW(exchange);
    } finally {
      unacked.decrementAndGet();
    }
  }

  /**
   * Sends a request and returns the response exchange. A failed request throws, unless retries are
   * on: then it counts as a failed attempt and this returns {@code null}, so the caller sends it
   * again later; it throws once too many attempts in a row have failed. While backing off it waits
   * up to {@code waitMs} and returns {@code null}. Called on the fetcher thread.
   */
  public Exchange sendWithRetries(String uri, Processor request, long waitMs) throws IOException {
    if (pollErrors != null) {
      pollErrors.checkAttempts();
      if (!pollErrors.attemptAllowed()) {
        sleepQuietly(waitMs);
        return null;
      }
    }
    Exception failure;
    try {
      Exchange response = producer().send(uri, ExchangePattern.InOut, request);
      failure = response.getException();
      if (failure == null) {
        if (pollErrors != null) {
          pollErrors.succeeded();
        }
        return response;
      }
    } catch (RuntimeException e) {
      failure = e;
    }
    if (pollErrors == null) {
      throw new IOException("Request to " + URISupport.sanitizeUri(uri) + " failed", failure);
    }
    pollErrors.failed(failure);
    pollErrors.checkAttempts();
    return null;
  }

  private ProducerTemplate producer() {
    consumer();
    synchronized (this) {
      if (producer == null) {
        producer = context.createProducerTemplate();
      }
      return producer;
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Acknowledges the exchanges on the acker thread, in order. */
  public void ackAsync(List<Exchange> exchanges) {
    acker.execute(
        () -> {
          for (Exchange exchange : exchanges) {
            try {
              consumer.doneUoW(exchange);
            } catch (Exception e) {
              LOG.warn("Failed to acknowledge exchange {}", exchange.getExchangeId(), e);
            } finally {
              unacked.decrementAndGet();
            }
          }
          if (jmsConsumer != null) {
            jmsConsumer.acknowledged(exchanges.size());
          }
        });
  }

  private ConsumerTemplate consumer() {
    ConsumerTemplate current = consumer;
    if (current == null) {
      synchronized (this) {
        if (consumer == null) {
          start();
        }
        current = consumer;
      }
    }
    return current;
  }

  private void start() {
    DefaultCamelContext camel = new DefaultCamelContext();
    // Components are discovered through the job's classloader, not Flink's.
    camel.setApplicationContextClassLoader(userClassLoader);
    // Beans and component options must be in place before components start: some, such as
    // salesforce, log in when they start.
    camel.init();
    bindBeans(camel);
    configureComponents(camel);
    camel.start();
    if (endpointUri != null) {
      Endpoint endpoint = camel.getEndpoint(endpointUri);
      try {
        endpointSetup.configure(endpoint);
      } catch (Exception e) {
        throw new IllegalStateException(
            "Cannot configure " + URISupport.sanitizeUri(endpointUri), e);
      }
      if (pollErrors != null) {
        reportErrorsTo(endpoint, pollErrors);
      }
      if (isJms(endpoint)) {
        jmsConsumer = new TransactedJmsConsumer(endpoint);
      }
    }
    context = camel;
    consumer = camel.createConsumerTemplate();
    LOG.info("Started CamelContext {}", camel.getName());
  }

  /** Binds the result of each of the source's {@link Bind} methods in Camel's registry. */
  private void bindBeans(CamelContext camel) {
    Set<String> names = new HashSet<>();
    for (Method method : source.getClass().getMethods()) {
      Bind bind = method.getAnnotation(Bind.class);
      if (bind == null) {
        continue;
      }
      if (method.getParameterCount() != 0) {
        throw new IllegalStateException("@Bind method " + method + " must take no arguments");
      }
      if (!names.add(bind.value())) {
        throw new IllegalStateException(
            "@Bind(\"" + bind.value() + "\") is on more than one method, including " + method);
      }
      try {
        camel.getRegistry().bind(bind.value(), method.invoke(source));
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("@Bind method " + method + " failed", e);
      }
    }
  }

  /** Sets the component options on their components. An unknown option fails with its name. */
  private void configureComponents(CamelContext camel) {
    Map<String, Map<String, Object>> optionsByComponent = new HashMap<>();
    componentOptions.forEach(
        (key, value) -> {
          int dot = key.indexOf(COMPONENT_SEPARATOR);
          if (dot <= 0) {
            throw new IllegalArgumentException(
                "Component option '" + key + "' must be <component>.<option>");
          }
          optionsByComponent
              .computeIfAbsent(key.substring(0, dot), component -> new HashMap<>())
              .put(key.substring(dot + 1), value);
        });
    optionsByComponent.forEach(
        (name, options) -> {
          Component component = camel.getComponent(name);
          if (component == null) {
            throw new IllegalArgumentException(
                "No Camel component '"
                    + name
                    + "' on the classpath for options "
                    + options.keySet());
          }
          PropertyBindingSupport.build()
              .withCamelContext(camel)
              .withTarget(component)
              .withProperties(options)
              .withMandatory(true)
              .withRemoveParameters(false)
              .bind();
        });
  }

  /**
   * Whether the endpoint belongs to camel-jms or a component built on it. Compares class names,
   * because camel-jms is only on the classpath of jobs that read from JMS.
   */
  private static boolean isJms(Endpoint endpoint) {
    for (Class<?> type = endpoint.getClass(); type != null; type = type.getSuperclass()) {
      if (type.getName().equals(JMS_ENDPOINT_CLASS)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Hooks fetch errors into {@code pollErrors} before ConsumerTemplate creates the consumer.
   * Scheduled poll consumers report failed polls to their poll strategy; event-driven consumers
   * only to their exception handler. A user-configured strategy or handler is left alone.
   */
  private void reportErrorsTo(Endpoint endpoint, PollErrors pollErrors) {
    // FTP and SFTP skip a poll they can't connect or log in for, logging one WARN, unless told
    // to throw.
    if (!endpointUri.contains("throwExceptionOnConnectFailed")) {
      PropertyConfigurer configurer = endpoint.getComponent().getEndpointPropertyConfigurer();
      if (configurer != null) {
        configurer.configure(
            endpoint.getCamelContext(), endpoint, "throwExceptionOnConnectFailed", true, true);
      }
    }
    if (endpoint instanceof ScheduledPollEndpoint scheduled) {
      if (scheduled.getPollStrategy() == null) {
        scheduled.setPollStrategy(pollErrors);
      }
    } else if (endpoint instanceof DefaultEndpoint other
        && other.getExceptionHandler() == null
        && !other.isBridgeErrorHandler()) {
      other.setExceptionHandler(pollErrors);
    }
  }

  /**
   * Fires every ack already handed over, then stops Camel. Exchanges that were received but never
   * covered by a completed checkpoint are not acknowledged, so the source system redelivers them.
   */
  @Override
  public void close() throws Exception {
    acker.shutdown();
    if (!acker.awaitTermination(PENDING_ACKS_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
      LOG.warn("Timed out waiting for pending Camel acks");
      acker.shutdownNow();
    }
    synchronized (this) {
      if (producer != null) {
        producer.close();
      }
      if (jmsConsumer != null) {
        jmsConsumer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
      if (context != null) {
        context.close();
      }
    }
  }
}
