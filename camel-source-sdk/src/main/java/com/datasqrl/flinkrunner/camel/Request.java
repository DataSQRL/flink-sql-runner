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
package com.datasqrl.flinkrunner.camel;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A request a {@link CursorSource} sends through Camel: a producer endpoint URI plus an optional
 * body and headers. Queries belong in the body or headers, because real queries don't survive being
 * URL-encoded into an endpoint string.
 *
 * <pre>{@code
 * Request.to("salesforce:queryAll?rawPayload=true").body("SELECT Id FROM Account WHERE ...");
 * }</pre>
 */
public final class Request {

  private final String uri;
  private final Object body;
  private final Map<String, Object> headers;

  private Request(String uri, Object body, Map<String, Object> headers) {
    this.uri = Objects.requireNonNull(uri, "uri");
    this.body = body;
    this.headers = headers;
  }

  public static Request to(String uri) {
    return new Request(uri, null, Map.of());
  }

  /** A copy of this request with the given body. */
  public Request body(Object body) {
    return new Request(uri, body, headers);
  }

  /** A copy of this request with the header added. */
  public Request header(String name, Object value) {
    Map<String, Object> copy = new LinkedHashMap<>(headers);
    copy.put(name, value);
    return new Request(uri, body, Collections.unmodifiableMap(copy));
  }

  public String uri() {
    return uri;
  }

  public Object body() {
    return body;
  }

  public Map<String, Object> headers() {
    return headers;
  }

  @Override
  public String toString() {
    return "Request{" + uri + "}";
  }
}
