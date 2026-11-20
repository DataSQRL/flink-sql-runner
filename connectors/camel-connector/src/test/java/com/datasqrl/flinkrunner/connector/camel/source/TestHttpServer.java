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

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/** A local HTTP server for tests, answering every request on one path with a handler. */
public final class TestHttpServer implements AutoCloseable {

  public record Response(int status, String body, Map<String, String> headers) {}

  /** What the handler sees of a request. */
  public record Received(String query, Headers headers) {}

  private final HttpServer server;
  private final List<Received> requests = new CopyOnWriteArrayList<>();

  public TestHttpServer(String path, Function<Received, Response> handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(path, exchange -> handle(exchange, handler));
    server.start();
  }

  private void handle(HttpExchange exchange, Function<Received, Response> handler)
      throws IOException {
    Received request =
        new Received(exchange.getRequestURI().getRawQuery(), exchange.getRequestHeaders());
    requests.add(request);
    Response response;
    try {
      response = handler.apply(request);
    } catch (RuntimeException e) {
      response = new Response(500, e.toString(), Map.of());
    }
    response.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
    byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
    if (response.status() == 304) {
      exchange.sendResponseHeaders(304, -1);
    } else {
      exchange.sendResponseHeaders(response.status(), body.length == 0 ? -1 : body.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(body);
      }
    }
    exchange.close();
  }

  public String url(String path) {
    return "http://localhost:" + server.getAddress().getPort() + path;
  }

  public List<Received> requests() {
    return requests;
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
