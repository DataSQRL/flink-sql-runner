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

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;

/**
 * A component whose consumer pushes numbered exchanges as fast as they are taken, like an
 * event-driven consumer of a busy queue, and counts every exchange it pushes.
 */
public final class PushingComponent extends DefaultComponent {

  static final AtomicInteger PUSHED = new AtomicInteger();

  @Override
  protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
    return new PushingEndpoint(uri, this);
  }

  private static final class PushingEndpoint extends DefaultEndpoint {

    PushingEndpoint(String uri, Component component) {
      super(uri, component);
    }

    @Override
    public Producer createProducer() {
      throw new UnsupportedOperationException("The pushing component only consumes");
    }

    @Override
    public Consumer createConsumer(Processor processor) {
      return new PushingConsumer(this, processor);
    }
  }

  private static final class PushingConsumer extends DefaultConsumer {

    private Thread pusher;

    PushingConsumer(Endpoint endpoint, Processor processor) {
      super(endpoint, processor);
    }

    @Override
    protected void doStart() throws Exception {
      super.doStart();
      pusher = new Thread(this::push, "pushing-consumer");
      pusher.setDaemon(true);
      pusher.start();
    }

    private void push() {
      while (!Thread.currentThread().isInterrupted()) {
        Exchange exchange = createExchange(false);
        exchange.getMessage().setBody(String.valueOf(PUSHED.incrementAndGet()));
        try {
          getProcessor().process(exchange);
        } catch (Exception e) {
          return;
        }
      }
    }

    @Override
    protected void doStop() throws Exception {
      pusher.interrupt();
      super.doStop();
    }
  }
}
